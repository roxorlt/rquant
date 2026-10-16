import Foundation
import SwiftUI
import AppKit

@MainActor
final class Store: ObservableObject {
    @Published var payload: LivePayload?
    @Published var error: String?
    @Published var unread = 0
    @Published var peekAlert: WuhuAlert?
    @Published var lastOK: Date?

    private var seen = Set<String>()
    private var primed = false
    private var task: Task<Void, Never>?
    private var peekTask: Task<Void, Never>?
    private let session: URLSession = {
        let c = URLSessionConfiguration.ephemeral
        c.timeoutIntervalForRequest = 5
        c.requestCachePolicy = .reloadIgnoringLocalCacheData
        return URLSession(configuration: c)
    }()

    /// Alerts belonging to today's five tigers only.
    var wuhuAlerts: [WuhuAlert] { (payload?.alerts ?? []).filter { $0.in_wuhu } }

    func start() {
        task?.cancel()
        task = Task { [weak self] in
            while !Task.isCancelled {
                guard let self else { return }
                let trading = Config.replay || Self.isTradingWindow(Date())
                if trading || self.payload == nil { await self.fetch() }
                try? await Task.sleep(nanoseconds: (trading ? 3 : 60) * 1_000_000_000)
            }
        }
    }

    /// Mon–Fri 09:14–15:05 Asia/Shanghai.
    static func isTradingWindow(_ d: Date) -> Bool {
        var cal = Calendar(identifier: .gregorian)
        cal.timeZone = TimeZone(identifier: "Asia/Shanghai")!
        let c = cal.dateComponents([.weekday, .hour, .minute], from: d)
        guard let wd = c.weekday, (2...6).contains(wd), let h = c.hour, let m = c.minute else { return false }
        let t = h * 60 + m
        return t >= 9 * 60 + 14 && t <= 15 * 60 + 5
    }

    func fetch() async {
        let cfg = Config.load()
        guard let token = cfg.token else { error = "未配置 token"; return }
        var req = URLRequest(url: cfg.endpoint)
        req.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        do {
            let (data, resp) = try await session.data(for: req)
            guard let http = resp as? HTTPURLResponse, http.statusCode == 200 else {
                error = "HTTP \((resp as? HTTPURLResponse)?.statusCode ?? 0)"; return
            }
            let p = try JSONDecoder().decode(LivePayload.self, from: data)
            if p != payload { payload = p }
            error = nil; lastOK = Date()
            ingestAlerts(p)
        } catch {
            self.error = "离线"
        }
    }

    private func ingestAlerts(_ p: LivePayload) {
        let mine = p.alerts.filter { $0.in_wuhu }
        let fresh = mine.filter { !seen.contains($0.id) }
        fresh.forEach { seen.insert($0.id) }
        guard primed else { primed = true; return }   // don't flash history on launch
        if let newest = fresh.first {
            unread += fresh.count
            peek(newest)
        }
    }

    private func peek(_ a: WuhuAlert) {
        peekAlert = a
        peekTask?.cancel()
        peekTask = Task { [weak self] in
            try? await Task.sleep(nanoseconds: 3_000_000_000)
            if !Task.isCancelled { self?.peekAlert = nil }
        }
    }

    func markRead() { unread = 0 }

    func open(_ code: String?) {
        var s = Config.appURL
        if let code { s += "#/stock/\(code)" }
        if let u = URL(string: s) { NSWorkspace.shared.open(u) }
    }
}
