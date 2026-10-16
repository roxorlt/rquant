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
    let bias: String?
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
