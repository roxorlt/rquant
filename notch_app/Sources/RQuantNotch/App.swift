import AppKit
import SwiftUI
import ServiceManagement

final class NotchPanel: NSPanel {
    override var canBecomeKey: Bool { false }
    override var canBecomeMain: Bool { false }
}

@main
@MainActor
final class AppDelegate: NSObject, NSApplicationDelegate {
    static func main() {
        let app = NSApplication.shared
        let d = AppDelegate()
        app.delegate = d
        app.setActivationPolicy(.accessory)   // no Dock icon
        app.run()
    }

    var panel: NotchPanel!
    let store = Store()
    var statusItem: NSStatusItem?
    /// Mirrors RootView hover so collapsed hit-testing can shrink to the notch.
    var hovering = false
    private var mouseMonitor: Any?
    private var localMouseMonitor: Any?

    func applicationDidFinishLaunching(_ n: Notification) {
        registerLoginItem()
        let size = NSSize(width: Layout.expandedW + 40, height: 260)
        panel = NotchPanel(contentRect: NSRect(origin: .zero, size: size),
                           styleMask: [.borderless, .nonactivatingPanel], backing: .buffered, defer: false)
        panel.isOpaque = false
        panel.backgroundColor = .clear
        panel.hasShadow = false
        panel.level = NSWindow.Level(rawValue: NSWindow.Level.statusBar.rawValue + 1)  // above menu bar
        panel.collectionBehavior = [.canJoinAllSpaces, .stationary, .fullScreenAuxiliary, .ignoresCycle]
        panel.isMovable = false
        panel.hidesOnDeactivate = false
        panel.ignoresMouseEvents = true   // start pass-through; updateMouseThrough enables over notch
        panel.acceptsMouseMovedEvents = true
        let host = NSHostingView(rootView: RootView(
            store: store,
            onHover: { [weak self] h in self?.hovering = h; self?.updateMouseThrough() },
            onSize: { _ in }
        ))
        host.frame = NSRect(origin: .zero, size: size)
        panel.contentView = host
        position()
        updateVisibility()
        Timer.scheduledTimer(withTimeInterval: 30, repeats: true) { [weak self] _ in
            let s = self; Task { @MainActor in s?.updateVisibility() }
        }
        NotificationCenter.default.addObserver(self, selector: #selector(position),
                                               name: NSApplication.didChangeScreenParametersNotification, object: nil)
        setupMenu()
        store.start()
        // Global monitor: needed while ignoresMouseEvents == true (window gets no local moves).
        mouseMonitor = NSEvent.addGlobalMonitorForEvents(matching: [.mouseMoved, .leftMouseDragged]) { [weak self] _ in
            Task { @MainActor in self?.updateMouseThrough() }
        }
        localMouseMonitor = NSEvent.addLocalMonitorForEvents(matching: [.mouseMoved, .leftMouseDragged]) { [weak self] e in
            let s = self
            Task { @MainActor in s?.updateMouseThrough() }
            return e
        }
        // Slow fallback if a monitor is dropped.
        Timer.scheduledTimer(withTimeInterval: 0.25, repeats: true) { [weak self] _ in
            let s = self; Task { @MainActor in s?.updateMouseThrough() }
        }
        updateMouseThrough()
    }

    func updateVisibility() {
        let show = Config.replay || Store.isTradingWindow(Date())
        if show { if !panel.isVisible { panel.orderFrontRegardless() } }
        else if panel.isVisible { panel.orderOut(nil) }
    }

    /// Screen with a notch if present (built-in display), else main.
    var targetScreen: NSScreen {
        NSScreen.screens.first(where: { $0.safeAreaInsets.top > 0 }) ?? NSScreen.main ?? NSScreen.screens[0]
    }

    @objc func position() {
        let s = targetScreen
        let top = s.safeAreaInsets.top
        Layout.notchH = top > 0 ? top : (s.frame.maxY - s.visibleFrame.maxY)  // notch height or menu bar
        if let left = s.auxiliaryTopLeftArea, let right = s.auxiliaryTopRightArea {
            Layout.notchW = max(right.minX - left.maxX, 120)
        } else {
            Layout.notchW = 200
        }
        let f = panel.frame
        panel.setFrameOrigin(NSPoint(x: s.frame.midX - f.width / 2, y: s.frame.maxY - f.height))
        updateMouseThrough()
    }

    /// Physical notch (or menu-bar strip) in global Cocoa coords.
    func notchHitRect() -> NSRect {
        let s = targetScreen
        let h = Layout.notchH
        if let left = s.auxiliaryTopLeftArea, let right = s.auxiliaryTopRightArea {
            return NSRect(x: left.maxX, y: s.frame.maxY - h, width: right.minX - left.maxX, height: h)
        }
        let w = Layout.notchW
        return NSRect(x: s.frame.midX - w / 2, y: s.frame.maxY - h, width: w, height: h)
    }

    func peekHitRect() -> NSRect {
        let f = panel.frame
        return NSRect(x: f.midX - Layout.peekW / 2, y: f.maxY - Layout.peekH,
                      width: Layout.peekW, height: Layout.peekH)
    }

    func expandedHitRect() -> NSRect {
        let f = panel.frame
        // Visible expanded panel only (not the full transparent window).
        let h: CGFloat = min(240, f.height)
        return NSRect(x: f.midX - Layout.expandedW / 2, y: f.maxY - h,
                      width: Layout.expandedW, height: h)
    }

    /// Collapsed: only the real notch receives mouse events (Chrome tabs / menu bar pass through).
    /// Peek / expanded: only the visible black panel.
    func updateMouseThrough() {
        guard panel.isVisible else {
            if !panel.ignoresMouseEvents { panel.ignoresMouseEvents = true }
            return
        }
        let m = NSEvent.mouseLocation
        let hit: NSRect
        if hovering {
            hit = expandedHitRect()
        } else if store.peekAlert != nil {
            hit = peekHitRect()
        } else {
            hit = notchHitRect()
        }
        let inside = hit.contains(m)
        if panel.ignoresMouseEvents == inside {
            panel.ignoresMouseEvents = !inside
        }
    }

    func registerLoginItem() {
        if SMAppService.mainApp.status != .enabled {
            do { try SMAppService.mainApp.register() } catch { NSLog("rquant-notch login item: \(error)") }
        }
    }

    func setupMenu() {
        let item = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        item.button?.title = "虎"
        let menu = NSMenu()
        menu.addItem(withTitle: "打开 rQuant", action: #selector(openApp), keyEquivalent: "")
        menu.addItem(withTitle: "立即刷新", action: #selector(refresh), keyEquivalent: "r")
        menu.addItem(.separator())
        menu.addItem(withTitle: "退出", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        menu.items.forEach { if $0.action != #selector(NSApplication.terminate(_:)) { $0.target = self } }
        item.menu = menu
        statusItem = item
    }
    @objc func openApp() { store.open(nil) }
    @objc func refresh() { Task { @MainActor in await store.fetch() } }
}
