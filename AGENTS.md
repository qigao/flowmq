# AGENTS.md

## 职责与优先级

本文件是 Salts 及关联工程的工作入口：保留仓库级约束，按任务引导使用专项 skills。API 细节、模块协议、构建步骤与测试写法由对应 skill 及其 references 维护，不在此复制一份。

- 研究先于编码，证据先于判断，兼容先于重写，验证先于宣告完成。
- 用户明确要求优先于本文件及 skills 中的一般规范；仓库实际契约优先于通用示例。
- 默认保持用户可见行为稳定。正确性、数据一致性与可验证性优先于性能和形式上的统一。
- 遇到规则冲突，说明冲突与影响，选择符合已授权目标且较少破坏现有行为的方案；无法判断真实意图时再询问用户。

## 工作流程

1. 明确目标、影响范围与已有授权，检查工作区状态和目标目录内适用的指引。
2. 按下表选择与任务直接相关的 skills，读取其 `SKILL.md`，再按其中路由读取必要 references。
3. 阅读当前 checkout 的公开头文件、实现、测试与调用点；编码前至少检查 3 处相关位置，明确输入输出、依赖、配置来源和用户可见行为。
4. 确认模块归属、状态所有者、错误语义与兼容性，优先复用既有能力，实施最小完整改动。
5. 先运行最小相关验证，再按影响面扩大回归；最后报告改动、证据及未验证范围。

纯文档任务核对相关规则、引用与事实源，不为满足流程而修改代码或运行无关构建。

## Skills 的选择与使用

### 使用方式

- 用户显式指定 skill（如 `$cnet`）时使用该 skill；未指定时按实际修改内容选择，不因关键词相似就加载整套技能。
- 从当前会话的技能目录解析名称和 `SKILL.md` 位置，不在仓库中硬编码个人安装路径。首次使用时简短告知用户。
- 先选模块 skill，再补充本次操作涉及的横向 skill。例如修复 CNet 的 buffer 生命周期，使用 `cnet` + `memory-design-protocols`；修改测试时加 `tinytest`，执行构建测试时加 `cmake-presets`。
- 遵守 skill 中明确要求的关联技能和 reference 路由；只读取本次工作需要的资料，不递归加载无关内容。
- Skill 是专项工作指引，不能替代当前公开头文件、实现、测试和调用点。示例不证明功能已存在，不照搬旧路径、API、target 或 preset 名称。
- 若 skill 缺失或路径失效，先按会话技能目录查找；仍不可用时说明缺失及影响。可继续不依赖它的工作；若必要规范无法核实，先请求补齐，不猜测其内容或擅自安装替代品。
- 维护规则时：通用专项规范归对应 skill；本文件只保留触发条件、仓库约束和稳定入口。修改全局 skill 需属于用户授权范围。

### 模块路由

| 任务内容 | 使用 skill | 关注边界 |
| --- | --- | --- |
| C11 元编程、Schema/Replay、Reflection、Enum/Struct、traits、接口、typed callable、Function ABI、DataDesc/ObjectRef、资源作用域 | `cmeta` | 元数据与运行时归属、跨翻译单元语义、ABI、反射权限与生命周期 |
| raw/typed 容器、Vec/List/Deque/Map/Set、managed 元素、range/collector | `cstl` | 容器算法、元素生命周期、容量、借用失效 |
| Stream/Graph、typed operators、lowering、优化、解释或编译执行、Source/Scheduler | `cflow` | 图与执行状态、效果约束、demand、执行所有权 |
| Publisher/Subscriber/Subscription、WAIT/waker、背压、replay/cache、多播、取消和终止 | `reactive` | demand 与终止协议、retained 值、关闭 |
| Actor、Machine/Statechart actor、mailbox、retained refs、监督重启、IO Actor | `actor` | 生命周期、代际隔离、消息与 IO 归属 |
| `coroutine/` 的有界分片协程 executor、await handle、完成竞争、timeout、drain | `executor` | shard 亲和、任务终态、外部完成与销毁 |
| CNet client/listener、TCP/UDP/Pipe、TLS、WebSocket、DNS、poll、发送接纳与完成 | `cnet` | progress owner、连接句柄、payload、取消和关闭 |
| Plugin 声明与跨 TU 导出、ABI 校验、loader/registry、lease、停止卸载；插件依赖、隔离或热重载设计 | `plugin-system`；涉及反射与调用契约时加 `cmeta` | 已实现的发布/生命周期协议与扩展设计分开，保留版本与借用边界 |

组合边界：CSTL Stream 使用 `cstl` + `cflow`；改动类型描述符或生成宏时加 `cmeta`；CFlow 自身的 scheduler/executor 使用 `cflow`，只有涉及独立协程 executor 时才加 `executor`。

### 横向路由

| 任务内容 | 使用 skill |
| --- | --- |
| 内存所有权、buffer/view、ring/queue、pool、跨线程传递、容量、背压、shutdown/drain | `memory-design-protocols` |
| profiling、热路径、SIMD、缓存布局、分配或锁竞争优化、benchmark 设计与解释 | `performance-optimization`；涉及数据生命周期时同时使用 `memory-design-protocols` |
| 新增、删除、修改或审查日志，日志级别、采样、上下文、sink、轮转、审计与可靠交付 | `logging-guide` |
| configure/build/test/install/package、presets、vcpkg、target/test 注册、CI、build tree 恢复 | `cmake-presets` |
| TinyTest 测试、断言、fixture、benchmark，TinyMock 反射 mock 或函数替换 | `tinytest`；运行构建测试时同时使用 `cmake-presets` |
| C 模块职责、依赖、接口、Result、设计模式或过度抽象审查 | `c-design-patterns` |
| C++ 专属设计、RAII、Pimpl、类型擦除或模式选择 | `cpp-design-patterns` |
| C++ 模板、traits、Concepts、constexpr、编译期计算或泛型代码 | `cpp-tmp` |

涉及对应工程或其集成时：独立 Chttp 的 HTTP/JSON-RPC/S3/OpenAPI 用 `chttp`；TurboParser DataBind 的 schema、绑定与序列化用 `databind-serialization`。不要仅为使用 skill 引入新的 API 或依赖，也不要将普通 C 序列化任务直接视为 DataBind 任务。

## 共享能力入口

优先复用 Salts 及项目内已有能力，再考虑已接入的 vendor/vcpkg 依赖和标准库。复用必须符合模块依赖方向，基础层不得为复用上层能力引入循环依赖。底层系统 API 封装在平台、协程或明确的项目适配层中。

下列能力与源码入口属于 Salts，不要求消费工程具有相同目录。本文的 Salts 资料链接按同级 `salts/` checkout 定位；消费方还应核对其实际使用的 SDK 版本、公开头文件与当前工程构建配置。

| 能力 | 默认入口与约束 |
| --- | --- |
| 拥有存储的可变字符串、拼接与构造 | [`tstr.h`](../salts/utils/include/tstr.h) 的 `tstr`；明确唯一所有权、扩容返回值与释放责任 |
| 借用字符串或字节视图、切片与只读参数 | [`vstr.h`](../salts/utils/include/vstr.h) 的 `vstr`；使用显式长度，底层存储必须覆盖借用期，不假设 NUL 结尾 |
| 类型化格式化与字符串格式化追加 | 项目 [`fmt.h`](../salts/utils/include/fmt.h)；复用其 `tstr`/`vstr` 集成，不另写格式化器或固定临时缓冲区拼接链 |
| 诊断日志 | [`tlog.h`](../salts/utils/include/tlog.h) + `logging-guide`；不另建日志系统，不用 `printf`/`fprintf` 代替业务诊断日志 |
| 有限泛型、Reflection、traits、接口与类型化调用 | [CMeta](../salts/cmeta/README.md) + `cmeta`；链接 `Salts::CMeta`，复用统一声明和描述符，不另建反射或类型系统 |
| RAII 与结构化资源清理 | CMeta 的 [scope](../salts/cmeta/include/cmeta/scope.h)、[ObjectRef scope](../salts/cmeta/include/cmeta/object_scope.h) 和 Plugin 的 [lease scope](../salts/plugin/include/salts/plugin_scope.h)；按资源归属选入口 |
| 动态模块发布、发现、契约校验与生命周期 | [Plugin](../salts/plugin/README.md) + `plugin-system`；发布方用 `Salts::PluginABI`，宿主运行时用 `Salts::Plugin` |
| 标准容器与 Stream facade | `cstl`；通过公开 typed/raw 入口复用，不手写已有动态数组、哈希表或双端队列 |
| buffer、arena/slab/object pool、ring/queue | `memory-design-protocols`；按生命周期、线程拓扑与消费语义选已有原语 |
| 文件、线程与其他共享能力 | 当前模块公开头文件与 README；文件操作优先复用 [`cmeta_fs.h`](../salts/utils/include/cmeta_fs.h)，线程池等能力先检索既有实现 |

字符串统一使用 `tstr`/`vstr`，格式化使用本项目 `fmt.h`，日志使用 `tlog`。新代码不以裸缓冲区拼接、直接使用 sds 或引入另一套字符串/格式化库替代这些入口；第三方边界和基础实现内部保留必要的底层操作，不能为了统一形式改坏既有 ABI。

- `tstr` 扩容可能使旧地址及其视图失效；按具体函数契约接收返回值和处理失败。
- `vstr` 不拥有也不延长源存储寿命。跨 callback、挂起、队列或 owner 边界保留内容时，按 `memory-design-protocols` 明确复制或保活协议。
- 格式化结果、长度、容量、终止与截断按当前 API 契约处理；不得因换用某个函数就省略边界检查。
- 上述字符串、格式化与日志能力由 `Salts::Core` 提供；容器通过 `Salts::CSTL`，其他能力链接各自模块的公开 target，按当前 CMake 定义核对。

### CMeta：Reflection 与类型语义

- **声明与生成**：复用有限 PP 原语、Schema/Replay、结构体/枚举/flags、traits、接口和 `cmeta_type`。CMeta 提供 Pair/Option/Result 等值类型的声明能力；容器算法与存储归 CSTL，图和执行归 CFlow。
- **Reflection**：用统一的不可变描述符表示类型、字段、函数签名、接口及数据语义；通过语义校验、相等性和派生查询消费元数据，跨 TU/DSO 不以描述符地址判断类型相等。入口见 [反射查询契约](../salts/cmeta/INSPECTION.md)，不自行维护第二套 RTTI、字段表或签名表。
- **已有类型接入**：`cmeta_reflect_data` 为已有 C/C++ 类型声明只读字段投影；需要完整字段级值生命周期时才显式使用 `cmeta_reflect_value`。反射可见性不授予写入、构造或执行权限，也不接管原对象所有权；约束见 [CMeta README](../salts/cmeta/README.md)。
- **对象与调用**：DataDesc、ObjectRef、Function/Invokable 复用同一类型和生命周期契约，支持受约束的对象访问、精确调用及校验后绑定复用。具体入口和 admission 条件由 `cmeta` skill 路由，不将裸函数地址视为已验证调用能力。

### RAII 与资源作用域

- **C 的结构化清理**：优先评估 `cmeta_scope`；它管理显式资源集合，构造失败时回滚，body 正常或提前返回后按逆序清理。运行时描述符走 `cmeta_scope_checked`；这不是任意 C 代码块的自动析构，不能用跨作用域跳转或 `longjmp` 绕过清理。详见 [structured scope](../salts/cmeta/LANGUAGE_REFERENCE.md#structured-scope)。
- **C++ RAII**：ObjectRef 使用 `cmeta::object_scope`，Plugin lease 使用 `salts::plugin_lease_scope`；复用既有不可复制、显式移动和析构释放契约。通用 C++ owner 设计再使用 `cpp-design-patterns`，不重复包装已有 owner。
- **释放顺序**：Data/ObjectRef/Plugin 的 cleanup 适配器复用显式清理义务，实际资源权威仍归原 owner。先销毁借用插件代码或数据的对象，再释放 lease，最后停止完成并卸载；scope 不自动延长所有外部借用的生命周期。按 `cmeta`、`plugin-system` 和 `memory-design-protocols` 核对失败与关闭协议。

### Plugin：声明发布与模块生命周期

- **声明和发布分层**：[`cmeta/plugin.h`](../salts/cmeta/include/cmeta/plugin.h) 的 `cmeta_plugin`、`cmeta_provides`、`cmeta_requires` 描述接口能力关系，属于静态反射元数据，不自动加载模块或解析依赖。真正发布 Function/Interface export 使用 [`salts/plugin_decl.h`](../salts/plugin/include/salts/plugin_decl.h) 的显式声明，复用 CMeta 精确签名和 Interface carrier。
- **导出聚合**：默认使用显式 export 表；需要跨 TU 分片时评估 [`plugin_linker.h`](../salts/plugin/include/salts/plugin_linker.h)，遵守平台支持、实际链接、预期数量及按 ID 查找的契约，不另建 constructor 驱动的全局注册表。
- **宿主运行时**：[`salts/plugin.h`](../salts/plugin/include/salts/plugin.h) 提供 manifest/ABI/contract 校验、动态加载、registry、启动、lease 获取释放、停止、quiescence 检查和卸载。registry 是模块与 lease 状态的事实源；只有满足终止与无在途使用条件才可卸载。
- **能力边界**：Plugin 复用 CMeta，但独立于 CFlow；反射查询和描述符复制不持有模块 lease。依赖求解、隔离、热重载及状态迁移须按当前实现核实并单独设计，不能把 skill 的通用方案当作现成运行时功能。

## 检索与结构理解

- 仓库文本和文件检索只使用 ripgrep/fd：Windows 使用 `rg.exe`/`fd.exe`，其他平台使用 `rg`/`fd`；文本优先 rg，文件优先 fd。
- 每个任务首次需要代码结构、调用关系或影响面分析时维护 CodeGraph：不存在 `.codegraph/` 则运行 `codegraph init -i .`，已存在则运行 `codegraph sync .`。
- 结构分析可用 `codegraph context -p . "<task>"`、`codegraph callers -p . <symbol>`、`codegraph callees -p . <symbol>`、`codegraph impact -p . <symbol>`、`codegraph affected -p . <files...>`。
- CodeGraph 仅辅助结构、影响面与测试候选分析，不替代文本检索和实际阅读。工具不可用或索引失败时说明原因，使用 rg/fd 与人工阅读继续。
- `.codegraph/` 是本地索引产物，不得提交。

## 修改边界与确认

默认在已有授权范围内推进可逆的阅读、审查、修改和验证。以下事项若尚未获得明确授权，先说明具体影响并询问：

- 删除或迁移用户数据，改变数据格式。
- 修改公开接口、协议语义、配置格式或部署方式。
- 高风险且不可逆的操作。

遇到与任务直接冲突、无法安全绕开的未知脏改动，或无法从上下文判断期望行为时，也需询问。已有授权不重复确认；先完成不依赖确认的工作。

不得使用 `git reset --hard`、`git checkout --` 或其他会静默抹除用户改动的命令。递归删除、批量移动或覆盖式写入前核实最终目标路径；只处理当前任务范围内的文件。

## 实现底线

专项设计、算法、生命周期与测试细则按 skills 执行；以下约束适用于所有改动。

- **职责与依赖**：先找既有归属模块，按业务能力、数据所有权或外部边界分层。跨模块走公开 API，依赖单向；不以全局单例或服务定位器隐藏依赖，不为了模式或减少行数制造抽象。
- **状态一致性**：同一状态只有一个主事实源，缓存与索引从它派生。分清纯判定、状态迁移和外部副作用；说明核心状态由谁拥有、何时提交、失败如何回滚、补偿或重试。
- **错误处理**：默认 fail fast，保留既有错误码/Result 契约。检查失败后再使用输出；中间层不能处理就传播，日志集中在消费或转换错误的边界，不记录后返回成功。资源清理保持单一明确的归属与路径。
- **Fallback**：仅在用户、协议、兼容性或设计文档明确要求时提供；说明触发条件、语义、差异、风险、测试及移除条件或长期保留依据。与主路径共享事实源，不掩盖不变量破坏、数据损坏或安全失败。
- **资源与并发**：跨模块/跨线程、有界或零拷贝数据路径先使用 `memory-design-protocols` 明确所有权、借用失效点、容量、背压、线程角色、同步和关闭协议。不得用无界增长、静默丢弃或提前回收掩盖资源不足。
- **输入与算术**：外部输入按契约校验；长度、容量、偏移等运算必须在溢出时明确失败。饱和运算只用于契约明确规定饱和语义的计数等场景，不能替代错误检查。
- **安全**：不得无故削弱认证、鉴权、TLS 验证、加密、审计或隔离。密钥不硬编码、不进入日志；加密复用成熟库。安全相关改动必须说明风险和验证方式。
- **性能与诊断**：优化先取证，使用 `performance-optimization`；保留必要的回调、所有权复制和兼容路径，不因“一刀切”的热路径禁令重写架构。日志用 `logging-guide` 控制逐事件成本，保留关键错误报告及必要审计交付，不把路径调用频率当作禁止错误日志的依据。
- **完整性**：不交付占位 API、伪实现、空函数或仅记录日志的未实现接口。阶段性内部实现标明范围与计划并隐藏，不能向用户暴露部分实现的公开 API。TODO/FIXME/HACK 需有 issue、移除条件或明确计划。
- **可维护性**：复用稳定概念，消除有意义的重复；命名业务常量，容量/阈值配置归属明确，不散落魔术值。测试数据和 Mock 留在模块现有测试目录，示例放入既有 `examples/` 并可独立构建运行。

涉及 ≥3 个模块、公开 API 语义、新依赖、性能/安全权衡或迁移成本 >2 人日的架构决策，需要在适当的现有文档中说明背景、候选方案、选择理由、取舍、迁移与回滚。分析对架构、算法、接口、状态归属、错误语义和测试范围的影响，不把大重构包装成必要补丁。

引入外部库前说明既有能力为何不足、替代方案和不引入的代价；检查许可证、维护与安全记录、版本、平台、ABI、线程模型、依赖体积、构建和测试。通过既有依赖入口接入并保留来源、版本、许可与本地修改记录，不复制来源不明的实现，不新增重复工具框架。

## 构建与验证

使用 `cmake-presets` 执行构建、测试与安装，使用 `tinytest` 编写或修改测试/benchmark/Mock。准确的 preset、target、测试名及 helper 签名以当前 checkout 为准。

- 复用正式测试覆盖行为、ownership、ABI、状态和失败路径；先最小相关测试，再相邻回归，按风险决定是否扩大。
- CI 的 configure 决定完整 build graph；`cmake --build` 构建该 graph，不在 workflow 中用 `--target` 或点名 target 缩减范围。
- CI 的测试和 benchmark 统一通过 CTest 执行，不直接运行 CMake 生成的 executable；test/benchmark、Debug/Release 用 configure 选项分离。
- 不用 CMake 读取源码/workflow 后以 `string(FIND)` 或 marker 证明行为正确；行为约束由正式 C/C++ 可执行测试验证。
- 每次改动给出可重复的验证步骤和实际结果。不能运行时说明原因、影响及剩余风险；未执行的验证不得写成通过。
- 文档改动检查引用、技能名称、规则一致性和 diff；不为低风险文字修改新增测试框架或临时验证工程。

## 交流与审查

- 默认使用简体中文；标识符、接口和既有文件语言遵循兼容性要求。
- 说明结构或边界问题时，同时说明影响哪些模块、接口、数据、测试或用户行为。
- 风险、发现与修复建议标注 `HIGH`（正确性、数据、安全、崩溃、接口破坏或重大回归）、`MED`（局部偏差、性能、维护或缺测）、`LOW`（命名、文档、可读性或局部一致性）。
- 区分证据：`事实` 引用文件/行号/测试/日志或一手资料；`计算` 给出输入与公式；`推论` 说明依据及不确定性；`常用做法` 不能替代仓库证据或契约。
- 审查先列发现，再给总体结论。每项包含触发条件、影响、证据和最小修复方向；无发现也说明已查范围、未查范围与残余风险。
- 交付说明改了什么、为何修改、如何验证以及限制；性能收益不能以推测代替测量。

## 文档与许可证

- 只新增任务需要的文档、日志和报告，不为流程创建 `.codex/` 或空壳产物。
- 注释解释意图、约束与设计理由；复杂算法说明复杂度和边界。API/行为改动同步文档、示例及废弃说明，不保留失效示例或空章节。
- API 文档包含参数、返回值、错误、所有权和示例；外部资料附链接。具体 API 用法以公开头文件为准，避免在此维护第二份 API 手册。
- first-party 源码、测试、构建文件不添加 `SPDX-License-Identifier` 或逐文件许可证模板；许可信息仅维护在根目录 `LICENSE`、`NOTICE`、`THIRD_PARTY_NOTICES.md`。
- vendored/third-party 文件保留上游版权、许可证与 SPDX 标识，不为统一格式删除或改写。
