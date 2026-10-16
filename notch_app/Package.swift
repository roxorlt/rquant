// swift-tools-version:5.9
import PackageDescription
let package = Package(
    name: "RQuantNotch",
    platforms: [.macOS(.v14)],
    targets: [.executableTarget(name: "RQuantNotch", path: "Sources/RQuantNotch")]
)
