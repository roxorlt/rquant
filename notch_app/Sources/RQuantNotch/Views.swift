import SwiftUI

// Plain muted palette (render6.py / tpl.html)
enum P {
    static let bg = Color.black
    static let text = Color(hex: 0xD1D1D6)
    static let strong = Color(hex: 0xE5E5EA)
    static let dim = Color(hex: 0x8E8E93)
    static let faint = Color(hex: 0x636366)
    static let faintest = Color(hex: 0x5A5A5F)
    static let rule = Color(hex: 0x2C2C2E)
    static let up = Color(hex: 0xC8645C)    // 红涨
    static let down = Color(hex: 0x5F9E76)  // 绿跌
    static func pct(_ v: Double?) -> Color { guard let v else { return dim }; return v > 0 ? up : (v < 0 ? down : dim) }
    static func bias(_ b: String?) -> Color {
        guard let b else { return dim }
        if b == "多" || b == "偏多" { return up }
        if b == "空" || b == "偏空" { return down }
        return dim
    }
    /// API sends 多/空; UI shows 偏多/偏空.
    static func biasLabel(_ b: String?) -> String {
        switch b {
        case "多": return "偏多"
        case "空": return "偏空"
        default: return b ?? ""
        }
    }
}

extension Color {
    init(hex: UInt32) { self.init(red: Double((hex >> 16) & 0xFF) / 255, green: Double((hex >> 8) & 0xFF) / 255, blue: Double(hex & 0xFF) / 255) }
}

func fmt(_ v: Double?, sign: Bool = true) -> String {
    guard let v else { return "--" }
    return String(format: sign ? "%+.2f" : "%.2f", v)
}

struct NotchShape: Shape {
    var radius: CGFloat
    var animatableData: CGFloat { get { radius } set { radius = newValue } }
    func path(in r: CGRect) -> Path {
        var p = Path()
        p.move(to: CGPoint(x: r.minX, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX, y: r.minY))
        p.addLine(to: CGPoint(x: r.maxX, y: r.maxY - radius))
        p.addQuadCurve(to: CGPoint(x: r.maxX - radius, y: r.maxY), control: CGPoint(x: r.maxX, y: r.maxY))
        p.addLine(to: CGPoint(x: r.minX + radius, y: r.maxY))
        p.addQuadCurve(to: CGPoint(x: r.minX, y: r.maxY - radius), control: CGPoint(x: r.minX, y: r.maxY))
        p.closeSubpath()
        return p
    }
}

struct Sparkline: View {
    let points: [Double]
    let ref: Double?
    var body: some View {
        GeometryReader { g in
            let pts = points
            if pts.count >= 2, let lo = (pts + [ref].compactMap { $0 }).min(), let hi = (pts + [ref].compactMap { $0 }).max() {
                let span = max(hi - lo, 1e-9)
                let y: (Double) -> CGFloat = { g.size.height - CGFloat(($0 - lo) / span) * g.size.height }
                ZStack {
                    if let ref {
                        Path { p in p.move(to: CGPoint(x: 0, y: y(ref))); p.addLine(to: CGPoint(x: g.size.width, y: y(ref))) }
                            .stroke(P.rule, style: StrokeStyle(lineWidth: 0.5, dash: [2, 2]))
                    }
                    Path { p in
                        for (i, v) in pts.enumerated() {
                            let pt = CGPoint(x: CGFloat(i) / CGFloat(pts.count - 1) * g.size.width, y: y(v))
                            if i == 0 { p.move(to: pt) } else { p.addLine(to: pt) }
                        }
                    }
                    .stroke(P.pct((pts.last ?? 0) - (ref ?? pts.first ?? 0)), lineWidth: 1)
                }
            }
        }
    }
}

/// Collapsed: the physical notch (no pixels there), plus a right "ear" outside the notch
/// that shows the unread 五虎-alert count during trading hours. Hotzone stays the notch only.
struct CollapsedView: View {
    @ObservedObject var store: Store
    static let ear: CGFloat = 30
    var showCount: Bool { store.unread > 0 && (Config.replay || Store.isTradingWindow(Date())) }
    var body: some View {
        HStack(spacing: 0) {
            Color.clear.frame(width: showCount ? Self.ear : 0)
            Color.clear.frame(width: Layout.notchW)
            if showCount {
                Text(store.unread > 99 ? "99+" : "\(store.unread)")
                    .font(.system(size: 11, weight: .semibold, design: .rounded))
                    .foregroundStyle(P.up)
                    .frame(width: Self.ear, alignment: .center)
            }
        }
        .frame(height: Layout.notchH)
    }
}

/// Peek: collapsed pill widened briefly showing the newest alert.
struct PeekView: View {
    let alert: WuhuAlert
    var body: some View {
        HStack(spacing: 8) {
            Text(alert.time.prefix(5)).foregroundStyle(P.faintest)
            Text(alert.name ?? alert.ts_code ?? "").foregroundStyle(P.strong).fontWeight(.medium)
            Text(alert.short).foregroundStyle(P.dim)
            Spacer()
            Text(P.biasLabel(alert.bias)).foregroundStyle(P.bias(alert.bias))
        }
        .font(.system(size: 11))
        .padding(.horizontal, 14)
        .padding(.top, Layout.notchH)
        .frame(width: Layout.peekW, height: Layout.peekH, alignment: .center)
    }
}

/// Expanded 440pt combined panel. Fixed height = header + 5 rows on both sides.
struct ExpandedView: View {
    @ObservedObject var store: Store
    static let rowH: CGFloat = 30
    static let headH: CGFloat = 14
    static let rows = 5
    var bodyH: CGFloat { Self.headH + CGFloat(Self.rows) * (Self.rowH + 0.5) }

    var body: some View {
        HStack(alignment: .top, spacing: 10) {
            tigerTable.frame(width: 238)
            Rectangle().fill(P.rule).frame(width: 0.5)
            alertList
        }
        .frame(height: bodyH, alignment: .top)
        .clipped()
        .padding(.top, Layout.notchH + 4)
        .padding(.horizontal, 12)
        .padding(.bottom, 8)
        .frame(width: Layout.expandedW, alignment: .topLeading)
    }

    func colHead<C: View>(@ViewBuilder _ c: () -> C) -> some View {
        HStack(spacing: 0) { c() }
            .font(.system(size: 8.5)).foregroundStyle(P.faint)
            .frame(height: Self.headH, alignment: .center)
    }

    var tigerTable: some View {
        VStack(alignment: .leading, spacing: 0) {
            colHead {
                Text("名称/代码").frame(width: 66, alignment: .leading)
                Text("竞价(%)").frame(width: 44, alignment: .trailing)
                Text("涨跌(%)").frame(width: 44, alignment: .trailing)
                Text("日内(%)").frame(width: 44, alignment: .trailing)
                Text("分时").frame(width: 40, alignment: .trailing)
            }
            ForEach(Array((store.payload?.wuhu ?? []).prefix(Self.rows))) { t in
                Divider().overlay(P.rule)
                Button { store.open(t.ts_code) } label: {
                    HStack(spacing: 0) {
                        VStack(alignment: .leading, spacing: 0) {
                            Text(t.name ?? "").font(.system(size: 10.5, weight: .medium)).foregroundStyle(P.strong).lineLimit(1)
                            Text(String(t.ts_code.prefix(6))).font(.system(size: 8, design: .monospaced)).foregroundStyle(P.faint)
                        }.frame(width: 66, alignment: .leading)
                        num(t.gap).frame(width: 44, alignment: .trailing)
                        num(t.pct_chg).frame(width: 44, alignment: .trailing)
                        num(t.intraday).frame(width: 44, alignment: .trailing)
                        Sparkline(points: t.spark, ref: t.pre_close)
                            .frame(width: 30, height: 12).frame(width: 40, alignment: .trailing)
                    }
                    .frame(height: Self.rowH)
                    .contentShape(Rectangle())
                }.buttonStyle(.plain)
            }
            let n = store.payload?.wuhu.count ?? 0
            if n < Self.rows {
                Divider().overlay(P.rule)
                Text(n == 0 ? (store.error ?? "暂无五虎") : "今日仅\(n)只入选")
                    .font(.system(size: 9)).foregroundStyle(P.faintest)
                    .frame(height: Self.rowH)
            }
        }
    }

    func num(_ v: Double?) -> some View {
        Text(fmt(v)).font(.system(size: 10, design: .monospaced)).foregroundStyle(P.pct(v))
    }

    /// 偏多 red, 偏空 green, neutral white.
    func nameColor(_ b: String?) -> Color {
        switch b {
        case "多", "偏多": return P.up
        case "空", "偏空": return P.down
        default: return P.strong
        }
    }

    var alertList: some View {
        VStack(alignment: .leading, spacing: 0) {
            colHead {
                Text("名称").frame(width: 46, alignment: .leading)
                Text("异动").frame(maxWidth: .infinity, alignment: .leading)
                Text("时间").frame(width: 28, alignment: .trailing)
            }
            let list = Array(store.wuhuAlerts.prefix(Self.rows))
            if list.isEmpty {
                Divider().overlay(P.rule)
                Text(store.payload?.alerts_available == false ? "今日暂无异动" : "暂无五虎异动")
                    .font(.system(size: 9.5)).foregroundStyle(P.faintest)
                    .frame(height: Self.rowH)
            }
            ForEach(list) { a in
                Divider().overlay(P.rule)
                Button { store.open(a.ts_code) } label: {
                    HStack(alignment: .center, spacing: 4) {
                        Text(a.name ?? "").font(.system(size: 10, weight: .medium))
                            .foregroundStyle(nameColor(a.bias)).lineLimit(1).minimumScaleFactor(0.8)
                            .frame(width: 46, alignment: .leading)
                        Text(a.short).font(.system(size: 9)).foregroundStyle(P.dim)
                            .lineLimit(2).minimumScaleFactor(0.85)
                            .frame(maxWidth: .infinity, alignment: .leading)
                        Text(a.time.prefix(5)).font(.system(size: 8.5, design: .monospaced)).foregroundStyle(P.faintest)
                            .frame(width: 28, alignment: .trailing)
                    }
                    .frame(height: Self.rowH)
                    .contentShape(Rectangle())
                }.buttonStyle(.plain)
            }
        }
    }
}

enum Layout {
    static var notchH: CGFloat = 32
    static var notchW: CGFloat = 200   // from auxiliaryTopLeft/Right; updated in AppDelegate.position()
    static let peekW: CGFloat = 300
    static let peekH: CGFloat = 56
    static let expandedW: CGFloat = 440
}

enum Mode { case collapsed, peek, expanded }

/// Expanded state, set by AppDelegate from mouse geometry (not by SwiftUI hover).
@MainActor
final class NotchUI: ObservableObject {
    @Published var expanded = false
}

struct RootView: View {
    @ObservedObject var store: Store
    @ObservedObject var ui: NotchUI
    var onHover: (Bool) -> Void = { _ in }
    var onSize: (CGSize) -> Void

    var mode: Mode { ui.expanded ? .expanded : (store.peekAlert != nil ? .peek : .collapsed) }

    var body: some View {
        VStack(spacing: 0) {
            Group {
                switch mode {
                case .collapsed: CollapsedView(store: store)
                case .peek: PeekView(alert: store.peekAlert!)
                case .expanded: ExpandedView(store: store)
                    .background(GeometryReader { g in
                        Color.clear
                            .onAppear { onSize(g.size) }
                            .onChange(of: g.size) { s in onSize(s) }
                    })
                }
            }
            .background(P.bg)
            .clipShape(NotchShape(radius: mode == .expanded ? 22 : 12))
            .shadow(color: .black.opacity(mode == .expanded ? 0.25 : 0), radius: 12, y: 6)
            .onHover { h in onHover(h) }   // AppDelegate decides expand/collapse from the mouse position
            Spacer(minLength: 0)
        }
        .frame(width: Layout.expandedW + 40, alignment: .top)
        .animation(.spring(response: 0.3, dampingFraction: 0.85), value: mode)
    }
}
