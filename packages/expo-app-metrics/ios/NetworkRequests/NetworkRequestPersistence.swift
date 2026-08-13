// Copyright 2025-present 650 Industries. All rights reserved.

import Foundation

/// Routes completed requests from `NetworkRequestMonitor` into the `spans` table, attributed to
/// the main session. This is the first span producer: it converts each request into a generic
/// `SpanRow` following the OTel HTTP semantic conventions, so the export layer (`expo-observe`)
/// ships spans without knowing what produced them. Unlike the in-memory ring buffer, the rows
/// survive process death until they're dispatched, pruned with their session, or displaced past
/// the table's row cap.
///
/// Installed on the monitor at launch (see `AppMetricsAppDelegateSubscriber`); the monitor calls
/// `persist` synchronously on `AppMetricsActor` for every completion it records.
@AppMetricsActor
final class NetworkRequestPersistence: Sendable {
  private let writer: SpanWriter
  private let sessionId: @Sendable () -> String

  /// Capture-time recording policy. Checked before each insert so disabling (or filtering)
  /// takes effect for future requests immediately; rows persisted earlier are untouched.
  private var configuration: NetworkSpansConfiguration

  /// `sessionId` is a closure so constructing the persistence doesn't force the session
  /// machinery into existence before the app delegate finished wiring it.
  init(
    writer: SpanWriter,
    configuration: NetworkSpansConfiguration = NetworkSpansConfiguration(),
    sessionId: @escaping @Sendable () -> String
  ) {
    self.writer = writer
    self.configuration = configuration
    self.sessionId = sessionId
  }

  /// Applies a new recording policy to subsequent requests. Called when JS reconfigures
  /// `traces.network`; the caller persists the value separately.
  func setConfiguration(_ configuration: NetworkSpansConfiguration) {
    self.configuration = configuration
  }

  /// Persists one completed request as a span: applies the recording policy, maps the snapshot
  /// onto the generic row shape, and hands it to the shared writer.
  func persist(_ request: NetworkRequest) {
    guard configuration.allows(url: request.url, method: request.method) else {
      return
    }
    guard let row = SpanRow.from(request: request, sessionId: sessionId()) else {
      return
    }
    writer.write(row)
  }
}

extension SpanRow {
  /// Builds a span row from a completed request snapshot, per the OTel HTTP semantic conventions
  /// for a client span: the span is named after the method (`HTTP` for a nonstandard one), a
  /// transport failure or a 4xx/5xx response makes it an ERROR, and each redirect hop becomes an
  /// `expo.http.redirect` event.
  ///
  /// The attribute keys mirror the set the ingestion endpoint extracts into dedicated columns
  /// (`http.request.method`, `url.full`, `server.address`, ...). `url.full` is redacted here, at
  /// the instrumentation, as the conventions require: userinfo credentials and the default
  /// sensitive query values never reach disk or the wire (ingestion redacts again as defense in
  /// depth). An intentionally canceled request keeps its span but stays UNSET with no
  /// `error.type`, per the conventions. RN apps cancel routinely, and counting aborts as errors
  /// would poison every error-rate view. `error.type` must stay low-cardinality: the captured
  /// `domain:code` pair for a transport failure, or the bare status code when the response itself
  /// was the error; the localized `errorDescription` goes to the status message instead.
  ///
  /// Returns `nil` when the snapshot carries no usable timestamps: without either endpoint of
  /// the request window there is nothing to anchor a span to. The capture factory always sets
  /// both, so this only guards directly constructed values.
  static func from(request: NetworkRequest, sessionId: String) -> SpanRow? {
    let start = request.timings.fetchStart
    let end = request.timings.responseEnd
    let duration = request.timings.totalDuration
    guard
      let resolvedStart = start ?? end?.addingTimeInterval(-duration),
      let resolvedEnd = end ?? start?.addingTimeInterval(duration)
    else {
      return nil
    }

    // Case-sensitive per the conventions: an unknown or nonstandard method becomes `_OTHER`
    // (verbatim value preserved in `http.request.method_original`) and names the span `HTTP`,
    // so caller-controlled verbs can't mint unbounded span names.
    let isKnownMethod = knownHttpMethods.contains(request.method)
    var attributes: [String: Any] = [
      "http.request.method": isKnownMethod ? request.method : "_OTHER",
      "url.full": redactedUrlFull(request.url),
    ]
    if !isKnownMethod {
      attributes["http.request.method_original"] = request.method
    }
    if let host = request.url.host {
      attributes["server.address"] = host
    }
    if let port = request.url.port ?? defaultPort(forScheme: request.url.scheme) {
      attributes["server.port"] = port
    }
    if let statusCode = request.statusCode {
      attributes["http.response.status_code"] = statusCode
    }
    if let version = semconvProtocolVersion(request.networkProtocol) {
      attributes["network.protocol.version"] = version
    }
    if let requestBytesSent = request.requestBytesSent {
      attributes["http.request.size"] = requestBytesSent
    }
    if let responseBytesReceived = request.responseBytesReceived {
      attributes["http.response.size"] = responseBytesReceived
    }
    // Not a semconv attribute: a cached response is timestamped and byte-counted like a download,
    // so without this a large cache hit reads as an impossible transfer rate. Omitted when the
    // platform did not classify the fetch, so its absence never implies a network load.
    if let fetchType = request.fetchType {
      attributes["expo.http.resource.fetch_type"] = fetchType.rawValue
    }
    let httpErrorStatus = (request.statusCode ?? 0) >= 400
    // An intentional cancellation is not a failure: status stays UNSET and `error.type` is
    // never set, per the conventions.
    let canceled = request.errorType == canceledErrorType
    if !canceled, let errorType = request.errorType ?? (httpErrorStatus ? request.statusCode.map(String.init) : nil) {
      attributes["error.type"] = errorType
    }

    let failed = !canceled && (request.errorDescription != nil || request.errorType != nil || httpErrorStatus)
    // The conventions model redirects as resent spans (`http.request.resend_count`), not
    // events; one span per chain is a deliberate deviation for this pipeline. The event and its
    // URL attributes are `expo.`-namespaced because the naming guidelines advise against putting
    // application-specific names under a semconv namespace like `http.`; the hop's status code
    // reuses the registry attribute it matches. A hop's arrival time becomes the event time;
    // without one the exporter anchors the event to the span start.
    let events: [[String: Any]] = request.redirects.map { redirect in
      var event: [String: Any] = [
        "name": "expo.http.redirect",
        "attributes": [
          "expo.http.redirect.from": redirect.fromUrl.absoluteString,
          "expo.http.redirect.to": redirect.toUrl.absoluteString,
          "http.response.status_code": redirect.statusCode,
        ],
      ]
      // Only a time that actually falls inside the span window can ship: an OTLP event outside
      // its own span has no valid place on the timeline. Dropping the key leaves the exporter to
      // anchor the event to the span start, which is where a clamp would have put it anyway.
      if let respondedAt = redirect.respondedAt?.unixMilliseconds,
        respondedAt >= resolvedStart.unixMilliseconds,
        respondedAt <= resolvedEnd.unixMilliseconds
      {
        event["timeMs"] = respondedAt
      }
      return event
    }

    return SpanRow(
      sessionId: sessionId,
      name: isKnownMethod ? request.method : "HTTP",
      kind: SpanRow.clientKind,
      startTimestampMs: resolvedStart.unixMilliseconds,
      endTimestampMs: resolvedEnd.unixMilliseconds,
      statusCode: failed ? SpanRow.statusError : nil,
      statusMessage: failed ? request.errorDescription : nil,
      attributes: serializeJSON(attributes),
      events: events.isEmpty ? nil : serializeJSON(events)
    )
  }

  /// The conventions' known method set: RFC 9110's methods plus PATCH and QUERY, matched
  /// case-sensitively as the conventions require.
  private static let knownHttpMethods: Set<String> = [
    "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH", "QUERY",
  ]

  /// `errorType` value NSURLSession reports for an intentional cancellation
  /// (`NSURLErrorCancelled`).
  private static let canceledErrorType = "NSURLErrorDomain:-999"

  /// Query parameter names whose values are redacted by default: the conventions' list for
  /// `url.full` (`X-Amz-Signature`, `X-Amz-Credential`, `X-Amz-Security-Token`, `sig`,
  /// `X-Goog-Signature`) plus the legacy S3 `AWSAccessKeyId` and `Signature` pair. Compared
  /// case-insensitively.
  private static let sensitiveQueryParameters: Set<String> = [
    "awsaccesskeyid", "signature", "sig", "x-amz-signature", "x-amz-credential", "x-amz-security-token",
    "x-goog-signature",
  ]

  /// `url.full` with userinfo credentials replaced by `REDACTED:REDACTED` and default-sensitive
  /// query values replaced by `REDACTED`, per the conventions. Redaction is the
  /// instrumentation's job, so secrets never reach the on-device database or the wire.
  private static func redactedUrlFull(_ url: URL) -> String {
    guard var components = URLComponents(url: url, resolvingAgainstBaseURL: false) else {
      // An unparseable URL can't prove it carries no secrets; drop the query outright.
      return url.absoluteString.components(separatedBy: "?").first ?? url.absoluteString
    }
    // Rewrite the components only when something was actually redacted, and go through
    // `percentEncodedQueryItems` rather than `queryItems`: the decoded form is not byte-faithful
    // (`a%2Bb` decodes to `a+b` and re-encodes as a literal `+`, which backends read as a space),
    // and that would corrupt the parameters this function keeps, not just the ones it replaces.
    // The sensitive names contain no characters that percent-encode, so matching the encoded name
    // is equivalent to matching the decoded one.
    var redactedAnything = false
    if components.user != nil || components.password != nil {
      components.user = "REDACTED"
      components.password = "REDACTED"
      redactedAnything = true
    }
    if let queryItems = components.percentEncodedQueryItems, !queryItems.isEmpty {
      var itemsChanged = false
      let redactedItems = queryItems.map { item -> URLQueryItem in
        // A valueless parameter (`?sig`) carries no secret, and giving it one would report a URL
        // the app never sent.
        guard sensitiveQueryParameters.contains(item.name.lowercased()), item.value != nil else {
          return item
        }
        itemsChanged = true
        return URLQueryItem(name: item.name, value: "REDACTED")
      }
      if itemsChanged {
        components.percentEncodedQueryItems = redactedItems
        redactedAnything = true
      }
    }
    guard redactedAnything else {
      return url.absoluteString
    }
    return components.string ?? url.absoluteString
  }

  /// Scheme-default port per semconv's `server.port` (a Required attribute for HTTP client
  /// spans), used when the URL carries no explicit port.
  private static func defaultPort(forScheme scheme: String?) -> Int? {
    switch scheme?.lowercased() {
    case "https", "wss":
      return 443
    case "http", "ws":
      return 80
    default:
      return nil
    }
  }

  /// Bare protocol version per semconv's `network.protocol.version` ("1.1", "2", "3"), mapped
  /// from the ALPN-style names the OS reports ("http/1.1", "h2", "h3"). Unrecognized values pass
  /// through verbatim rather than being dropped.
  private static func semconvProtocolVersion(_ networkProtocol: String?) -> String? {
    switch networkProtocol {
    case nil:
      return nil
    case "h2":
      return "2"
    case "h3":
      return "3"
    case .some(let other):
      return other.hasPrefix("http/") ? String(other.dropFirst("http/".count)) : other
    }
  }

  /// Serializes a JSON-compatible value built above; the inputs are all strings and numbers, so
  /// a failure means a programming error and degrading to `nil` (dropped blob) is safe.
  private static func serializeJSON(_ object: Any) -> String? {
    guard let data = try? JSONSerialization.data(withJSONObject: object) else {
      logger.warn("[AppMetrics] Failed to serialize span attributes")
      return nil
    }
    return String(data: data, encoding: .utf8)
  }
}

extension Date {
  /// Unix-epoch milliseconds, the integer form the spans table and the JS redirect record use.
  var unixMilliseconds: Int64 {
    return Int64((timeIntervalSince1970 * 1_000).rounded())
  }
}
