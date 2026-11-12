import Foundation
import Security

struct Tiger: Codable, Identifiable, Equatable {
    var id: String { ts_code }
    let rank: Int?
    let name: String?
    let ts_code: String
    let gap: Double?
    let price: Double?
    let open: Double?
    let pre_close: Double?
    let pct_chg: Double?
    let intraday: Double?
    let tick_time: String?
    let spark: [Double]
    let spark_m: [Int]?
    let bias: String?
}

/// Maps timestamps onto the fixed 240-minute A-share session width:
/// 09:30 -> 0.0, 11:30..13:00 (lunch) -> 0.5, 15:00+ -> 1.0.
enum SessionAxis {
    static let totalMinutes: Double = 240.0
    private static let shanghaiTZ = TimeZone(identifier: "Asia/Shanghai")!

    static func progress(timeString: String?) -> Double? {
        guard let s = timeString?.trimmingCharacters(in: .whitespaces), s.count >= 5 else { return nil }
        let parts = s.split(separator: ":")
        guard parts.count >= 2, let h = Int(parts[0]), let m = Int(parts[1]) else { return nil }
        let sec = parts.count >= 3 ? (Double(parts[2]) ?? 0) : 0
        return progress(minutesSinceMidnight: Double(h * 60 + m) + sec / 60.0)
    }

    static func progress(minutesSinceMidnight m: Double) -> Double {
        if m <= 9 * 60 + 30 { return 0 }
        if m <= 11 * 60 + 30 { return (m - 570.0) / totalMinutes }
        if m <= 13 * 60 { return 0.5 }
        if m < 15 * 60 { return (120.0 + (m - 780.0)) / totalMinutes }
        return 1.0
    }

    static func shanghaiDateAndMinutes(_ d: Date) -> (date: String, minutes: Double) {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = shanghaiTZ
        let c = cal.dateComponents([.year, .month, .day, .hour, .minute, .second], from: d)
        let ds = String(format: "%04d-%02d-%02d", c.year ?? 0, c.month ?? 0, c.day ?? 0)
        let mins = Double((c.hour ?? 0) * 60 + (c.minute ?? 0)) + Double(c.second ?? 0) / 60.0
        return (ds, mins)
    }

    /// Returns (dataProgress, sessionProgress) in [0, 1].
    /// - dataProgress: position of the last point in `tiger.spark` on the full-day axis.
    /// - sessionProgress: active session width at viewing/feed time (0.5 during lunch break, 1.0 after close).
    static func span(for tiger: Tiger, payload: LivePayload?, isOffline: Bool, now: Date = Date()) -> (data: Double, session: Double) {
        let fallbackCount = min(1.0, Double(max(0, tiger.spark.count - 1)) / totalMinutes)
        let tickP = progress(timeString: tiger.tick_time)
        let feedP = progress(timeString: payload?.feed_time)
        let dataP = tickP ?? feedP ?? fallbackCount

        if payload?.replay == true {
            return (dataP, max(dataP, feedP ?? dataP))
        }
        let (today, nowMins) = shanghaiDateAndMinutes(now)
        guard payload?.date == today else {
            return (dataP, max(dataP, feedP ?? 1.0))
        }
        if isOffline {
            return (dataP, max(dataP, feedP ?? dataP))
        }
        let wallP = progress(minutesSinceMidnight: nowMins)
        // If upstream feed is healthy (within 5 trading minutes of wall clock, including lunch/post-close),
        // anchor width to current viewing time; if upstream stalled mid-session, cap at feedP.
        let liveP: Double
        if let fp = feedP, wallP - fp > 5.0 / totalMinutes {
            liveP = fp
        } else {
            liveP = max(wallP, feedP ?? 0)
        }
        return (dataP, max(dataP, liveP))
    }
}

struct WuhuAlert: Codable, Identifiable, Equatable {
    var id: String { "\(seq ?? 0)|\(time)|\(ts_code ?? "")|\(type)" }
    let time: String
    let ts_code: String?
    let name: String?
    let type: String
    let short: String
    let level: Int?
    let bias: String
    let text: String?
    let in_wuhu: Bool
    let seq: Int?
}

struct LivePayload: Codable, Equatable {
    let date: String?
    let server_time: String?
    let feed_time: String?
    let feed_age_s: Double?
    let wuhu: [Tiger]
    let alerts: [WuhuAlert]
    let alerts_available: Bool?
    let replay: Bool?
}

enum Config {
    static let defaultEndpoint = "http://82.156.0.68:8081/api/v1/wuhu/live"
    static let appURL = "http://82.156.0.68:8081/app/"
    static let keychainService = "rquant-wuhu"
    static let keychainAccount = "token"
    static var configFile: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".config/rquant-notch/config.json")
    }

    /// Token: Keychain (service rquant-wuhu / account token) first, then ~/.config/rquant-notch/config.json {"token":..,"endpoint":..} (must be mode 600).
    /// Replay/demo mode: launch with --replay, or "replay":"1" in config.json, or `defaults write com.rquant.notch replay -bool YES`.
    static var replay: Bool {
        if CommandLine.arguments.contains("--replay") || UserDefaults.standard.bool(forKey: "replay") { return true }
        if let data = try? Data(contentsOf: configFile),
           let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            if let b = obj["replay"] as? Bool { return b }
            if let s = obj["replay"] as? String { return s == "1" || s == "true" }
        }
        return false
    }

    static func load() -> (endpoint: URL, token: String?) {
        var endpoint = defaultEndpoint
        var token: String? = keychainToken()
        if let data = try? Data(contentsOf: configFile),
           let obj = try? JSONSerialization.jsonObject(with: data) as? [String: Any] {
            if let e = obj["endpoint"] as? String, !e.isEmpty { endpoint = e }
            if token == nil, let t = obj["token"] as? String, !t.isEmpty {
                let perms = (try? FileManager.default.attributesOfItem(atPath: configFile.path)[.posixPermissions] as? Int) ?? 0
                if perms & 0o077 == 0 { token = t } else { NSLog("rquant-notch: config.json must be chmod 600; ignoring token") }
            }
        }
        if replay { endpoint += (endpoint.contains("?") ? "&" : "?") + "replay=1" }
        return (URL(string: endpoint)!, token)
    }

    static func keychainToken() -> String? {
        let q: [String: Any] = [kSecClass as String: kSecClassGenericPassword,
                                kSecAttrService as String: keychainService,
                                kSecAttrAccount as String: keychainAccount,
                                kSecReturnData as String: true,
                                kSecMatchLimit as String: kSecMatchLimitOne]
        var out: CFTypeRef?
        guard SecItemCopyMatching(q as CFDictionary, &out) == errSecSuccess, let d = out as? Data else { return nil }
        return String(data: d, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
    }
}
