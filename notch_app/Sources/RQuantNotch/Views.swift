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

/// Collapsed: sized to the physical notch (Layout.notchW × notchH).
struct CollapsedView: View {
    @ObservedObject var store: Store
    var body: some View {
        HStack {
            HStack(spacing: 4) {
                ForEach(store.payload?.wuhu ?? []) { t in
                    Circle().fill(P.bias(t.bias)).frame(width: 5, height: 5)
                }
                if store.payload == nil { Circle().fill(P.faint).frame(width: 5, height: 5) }
            }
            Spacer()
            if store.error != nil {
                Text("!").foregroundStyle(P.faint)
            } else if store.unread > 0 {
                Text("\(store.unread)").foregroundStyle(Color(hex: 0xAEAEB2))
            }
        }
        .font(.system(size: 10))
        .padding(.horizontal, 9)
        .frame(width: Layout.notchW, height: Layout.notchH)
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

/// Expanded 440pt combined panel.
struct ExpandedView: View {
    @ObservedObject var store: Store
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack(alignment: .top, spacing: 10) {
                tigerTable.frame(width: 238)
                Rectangle().fill(P.rule).frame(width: 0.5)
                alertList
            }
        }
        .padding(.top, Layout.notchH + 6)
        .padding(.horizontal, 12)
        .padding(.bottom, 12)
        .frame(width: Layout.expandedW, alignment: .topLeading)
    }

    var header: some View {
        HStack {
            Text((store.payload?.replay == true ? "五虎 · 回放" : "五虎 · 实时") + " · 竞价池 \(String((store.payload?.date ?? "").suffix(5)))")
            Spacer()
        }.font(.system(size: 9)).foregroundStyle(P.faintest)
    }

    var tigerTable: some View {
        VStack(alignment: .leading, spacing: 0) {
            header.padding(.bottom, 3)
            HStack(spacing: 0) {
                Text("名称/代码").frame(width: 66, alignment: .leading)
                Text("竞价(%)").frame(width: 44, alignment: .trailing)
                Text("涨跌(%)").frame(width: 44, alignment: .trailing)
                Text("日内(%)").frame(width: 44, alignment: .trailing)
                Text("分时").frame(width: 40, alignment: .trailing)
            }
            .font(.system(size: 8.5)).foregroundStyle(P.faint).padding(.bottom, 2)
            ForEach(store.payload?.wuhu ?? []) { t in
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
                    .padding(.vertical, 3)
                    .contentShape(Rectangle())
                }.buttonStyle(.plain)
            }
            let n = store.payload?.wuhu.count ?? 0
            if n < 5 {
                Text(n == 0 ? (store.error ?? "暂无五虎") : "今日仅\(n)只入选")
                    .font(.system(size: 9)).foregroundStyle(P.faintest).padding(.top, 5)
            }
        }
    }

    func num(_ v: Double?) -> some View {
        Text(fmt(v)).font(.system(size: 10, design: .monospaced)).foregroundStyle(P.pct(v))
    }

    var alertList: some View {
        VStack(alignment: .leading, spacing: 0) {
            HStack {
                Text("异动记录 · 最新在上")
                Spacer()
                Text(store.payload?.feed_time ?? "--:--:--")
            }.font(.system(size: 9)).foregroundStyle(P.faintest).padding(.bottom, 4)
            let list = Array(store.wuhuAlerts.prefix(5))
            if list.isEmpty {
                Text(store.payload?.alerts_available == false ? "今日暂无异动" : "暂无五虎异动")
                    .font(.system(size: 9.5)).foregroundStyle(P.faintest).padding(.top, 4)
            }
            ForEach(list) { a in
                Divider().overlay(P.rule)
                Button { store.open(a.ts_code) } label: {
                    VStack(alignment: .leading, spacing: 1.5) {
                        HStack(spacing: 6) {
                            Text(a.time.prefix(5)).foregroundStyle(P.faintest).font(.system(size: 9, design: .monospaced))
                            Text(a.name ?? "").foregroundStyle(P.strong).fontWeight(.medium).lineLimit(1)
                            Spacer(minLength: 2)
                            Text(P.biasLabel(a.bias)).foregroundStyle(P.bias(a.bias)).font(.system(size: 9))
                        }
                        Text(a.short).foregroundStyle(P.dim).font(.system(size: 9.5))
                            .lineLimit(2).fixedSize(horizontal: false, vertical: true)
                            .frame(maxWidth: .infinity, alignment: .leading)
                    }
                    .font(.system(size: 10))
                    .padding(.vertical, 4.5)
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

struct RootView: View {
    @ObservedObject var store: Store
    @State var hovering = false
    var onHover: (Bool) -> Void = { _ in }
    var onSize: (CGSize) -> Void

    var mode: Mode { hovering ? .expanded : (store.peekAlert != nil ? .peek : .collapsed) }

    var body: some View {
        VStack(spacing: 0) {
            Group {
                switch mode {
                case .collapsed: CollapsedView(store: store)
                case .peek: PeekView(alert: store.peekAlert!)
                case .expanded: ExpandedView(store: store)
                }
            }
            .background(P.bg)
            .clipShape(NotchShape(radius: mode == .expanded ? 22 : 12))
            .shadow(color: .black.opacity(mode == .expanded ? 0.25 : 0), radius: 12, y: 6)
            .onHover { h in
                withAnimation(.spring(response: 0.28, dampingFraction: 0.86)) { hovering = h }
                onHover(h)
                if h { store.markRead() }
            }
            Spacer(minLength: 0)
        }
        .frame(width: Layout.expandedW + 40, alignment: .top)
        .animation(.spring(response: 0.3, dampingFraction: 0.85), value: mode)
    }
}
