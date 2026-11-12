// Linux-runnable simulation of the notch hover/hit-test state machine (no AppKit).
// Mirrors App.swift updateMouseThrough/setExpanded (new) vs the 10-15 package and 10-16 hitfix logic.
// SwiftUI .onHover is modelled as: fires only while the window accepts mouse events (or always, in the
// pessimistic "tracking areas still fire" variant), over the currently rendered shape — which during the
// exit animation still includes the fading ExpandedView (removal transition).
import Foundation

let topY: CGFloat = 1000, midX: CGFloat = 0, winH: CGFloat = 260
let notchW: CGFloat = 190, notchH: CGFloat = 32, peekW: CGFloat = 300, peekH: CGFloat = 56, expW: CGFloat = 440
let exitAnim: Double = 0.6
func rect(_ w: CGFloat, _ h: CGFloat) -> CGRect { CGRect(x: midX - w / 2, y: topY - h, width: w, height: h) }
func hit(_ r: CGRect, _ m: CGPoint) -> Bool { CGRect(x: r.minX, y: r.minY, width: r.width, height: r.height + 1).contains(m) }

enum Impl: String { case pkg1015 = "旧包(10-15)", hitfix1016 = "仓库现状(10-16 hitfix)", fixed = "本次修复" }

final class Sim {
    let impl: Impl; let panelH: CGFloat; let trackingIgnoresFlag: Bool; let peek: Bool
    var ignores: Bool; var swiftUIHover = false     // RootView @State hovering (old) / ui.expanded (new)
    var collapsedAt: Double = -1e9; var t: Double = 0
    var log: [String] = []
    init(_ impl: Impl, panelH: CGFloat, trackingIgnoresFlag: Bool, peek: Bool = false) {
        self.impl = impl; self.panelH = panelH; self.trackingIgnoresFlag = trackingIgnoresFlag; self.peek = peek
        ignores = impl == .pkg1015 ? false : true
    }
    var expanded: Bool { swiftUIHover }
    var animating: Bool { t - collapsedAt < exitAnim }
    /// Shape SwiftUI hit-tests for hover (includes fading ExpandedView during exit animation).
    func swiftUIShape() -> CGRect {
        if expanded || animating { return rect(expW, min(panelH, winH)) }
        return peek ? rect(peekW, peekH) : rect(notchW, notchH)
    }
    func setHover(_ h: Bool, _ m: CGPoint) {
        guard h != swiftUIHover else { return }
        swiftUIHover = h
        if !h { collapsedAt = t }
        log.append(String(format: "t=%.3f y=%.0f %@", t, topY - m.y, h ? "展开" : "收起"))
    }
    // --- old: SwiftUI decides, AppDelegate mirrors ---
    func oldSwiftUIEvent(_ m: CGPoint) {
        guard !ignores || !trackingIgnoresFlag else { return }
        let inside = swiftUIShape().contains(m)
        if inside != swiftUIHover { setHover(inside, m); if impl == .hitfix1016 { hitfixUpdate(m) } }
    }
    func pkgUpdate(_ m: CGPoint) {
        let inside = rect(peekW, peekH).contains(m) || (!ignores && rect(expW, 240).contains(m))
        if ignores == inside { ignores = !inside }
    }
    func hitfixUpdate(_ m: CGPoint) {
        let r = swiftUIHover ? rect(expW, min(240, winH)) : (peek ? rect(peekW, peekH) : rect(notchW, notchH))
        let inside = r.contains(m); if ignores == inside { ignores = !inside }
    }
    // --- new: copy of App.swift logic ---
    func expandedHitRect() -> CGRect { rect(expW, min(panelH > 0 ? panelH : 240, winH)) }
    func newUpdate(_ m: CGPoint) {
        if expanded {
            if !hit(expandedHitRect(), m) { setHover(false, m); ignores = true }
        } else {
            var trigger = rect(notchW, notchH)
            if peek && !animating { trigger = trigger.union(rect(peekW, peekH)) }
            if hit(trigger, m) { setHover(true, m) }
        }
        let accept = expanded && hit(expandedHitRect(), m)
        if ignores == accept { ignores = !accept }
    }
    func move(_ m: CGPoint) {
        switch impl {
        case .pkg1015: pkgUpdate(m); oldSwiftUIEvent(m)
        case .hitfix1016: hitfixUpdate(m); oldSwiftUIEvent(m)
        case .fixed: newUpdate(m); _ = (!ignores || !trackingIgnoresFlag) ? newUpdate(m) : ()  // onHover nudge
        }
    }
    /// Path: enter notch → down through panel → out of bottom (+20) → quickly back up to just under old bottom edge.
    func run(returnTo: CGFloat, upDuration: Double) -> (reappeared: Bool, clickBlockedBelowNotch: Bool) {
        let dt = 0.004; var y: CGFloat = 5
        func at(_ y: CGFloat) -> CGPoint { CGPoint(x: midX + 10, y: topY - y) }
        move(at(y)); t += dt
        while y < panelH + 20 { y += 6; move(at(y)); t += dt }
        let collapsesBefore = log.filter { $0.contains("收起") }.count
        let steps = Int(upDuration / dt); let y0 = y
        var reappeared = false, blocked = false
        for i in 1...steps {
            let yy = y0 + (returnTo - y0) * CGFloat(i) / CGFloat(steps)
            move(at(yy)); t += dt
            if yy > notchH + 1 { if expanded { reappeared = true }; if !ignores && !expanded { blocked = true } ; if animating && !ignores { blocked = true } }
        }
        _ = collapsesBefore
        return (reappeared, blocked)
    }
}

var fails = 0
for panelH in [CGFloat(210), 290] {
    for strict in [true, false] {
        for impl in [Impl.pkg1015, .hitfix1016, .fixed] {
            for (ret, dur) in [(panelH - 10, 0.15), (panelH - 60, 0.25), (notchH + 4, 0.3)] {
                let s = Sim(impl, panelH: panelH, trackingIgnoresFlag: strict)
                let r = s.run(returnTo: ret, upDuration: dur)
                let tag = r.reappeared ? "✗ 刘海下方又展开" : "✓ 未重新展开"
                print("\(impl.rawValue) 面板高\(Int(panelH)) \(strict ? "追踪受ignores约束" : "追踪不受ignores约束(悲观)") 回到y=\(Int(ret)) 用时\(dur)s: \(tag)\(r.clickBlockedBelowNotch ? " · 退场时挡点击" : "")")
                if impl == .fixed && (r.reappeared || r.clickBlockedBelowNotch) { fails += 1; print(s.log) }
            }
        }
    }
}
// new impl: re-entering the physical notch during the exit animation must re-expand; peek pill must not.
do {
    let s = Sim(.fixed, panelH: 260, trackingIgnoresFlag: false, peek: true)
    _ = s.run(returnTo: 45, upDuration: 0.1)            // into peek pill (y=45 < 56) during animation
    if s.expanded { fails += 1; print("FAIL: peek pill re-expanded during exit animation") } else { print("✓ 退场动画中进入 peek 条(非刘海)不展开") }
    s.move(CGPoint(x: midX, y: topY - 10))                // now into notch
    if !s.expanded { fails += 1; print("FAIL: notch did not re-expand") } else { print("✓ 退场动画中进入刘海矩形立即重新展开") }
    s.move(CGPoint(x: midX, y: topY))                     // cursor pushed to y == maxY: must stay expanded
    if !s.expanded { fails += 1; print("FAIL: top edge collapsed") } else { print("✓ 光标顶到屏幕最上沿(y==maxY)不误收起") }
    s.t += 1; s.move(CGPoint(x: midX, y: topY - 300)); s.t += 1; s.move(CGPoint(x: midX + 120, y: topY - 45))
    if !s.expanded { fails += 1; print("FAIL: peek pill after animation should expand (existing behavior)") } else { print("✓ 动画结束后悬停 peek 条仍可展开(原有行为)") }
}
print(fails == 0 ? "ALL NEW-IMPL CHECKS PASS" : "FAILURES: \(fails)")
exit(fails == 0 ? 0 : 1)
