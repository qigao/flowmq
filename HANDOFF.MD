# Windows 并行 socket 测试的临时证书

普通 socket 和 peer pool 测试使用同一份 TLS 测试代码。两组并行运行时，曾在 `test_flowmq_socket.c` 的 `tt_remove_file(ca_path)` 处返回 -1，串行复跑通过。

所用 TinyTest 的 Windows 实现先通过 `GetTempFileNameA` 创建文件。调用者指定后缀时，它再通过 `MoveFileExA` 改名并允许替换已有目标。系统保证创建时的临时名唯一，改后缀后的路径可能被另一进程复用。TLS fixture 直接使用 TinyTest 创建的原始路径，证书内容仍为 PEM。

修订后执行 `ctest --preset native-release-user -LE benchmark -j 3 --output-on-failure`，两组 socket 测试均通过，功能回归共 50/50 项通过。

# llvm-mingw 的 TurboDB 发布输入

TurboDB.Native 0.1.1 的 Windows 资产按 RID 命名，导入的 MSVC 运行时和 DLL 名称与 llvm-mingw 的 Salts SDK 不匹配。恢复该包后，持久投递的 Windows 测试以 `0xc0000135` 退出。使用 TurboDB 0.1.2 发布清单内的 `windows-llvm-mingw-x64` SDK，并按该清单选择 SaltsBase 0.1.8、CMeta 0.1.7、SaltsUtils 0.1.5 和 SaltsCNet 0.1.15。发布恢复入口校验每个平台的依赖摘要。重新配置并构建 `ci-llvm-mingw-x64-release-user` 后，`ctest --preset ci-llvm-mingw-x64-release-user -L flowmq-delivery` 的两个测试均通过。
