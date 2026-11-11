# Central MVP — Thiết kế để duyệt

Trạng thái: **Baseline để lập implementation plan theo yêu cầu user**.
Ngày: 2026-11-09, múi giờ Asia/Bangkok.
Cập nhật phạm vi ngày 2026-11-10 theo yêu cầu user: Central cung cấp daemon,
CLI và API cho client độc lập; không xây hoặc đóng gói UI. P4 đổi thành một
task về API công khai và khả năng vận hành qua CLI. Các mockup UI được giữ làm
lịch sử khảo sát. Việc sửa tài liệu không phải chỉ thị triển khai code.

## 1. Mục đích và ranh giới

Central là nền tảng điều phối local, viết bằng Rust, cho một người giao việc
dài hạn cho các hệ thống agent có sẵn. Một run kéo dài vài giờ đến vài ngày,
có thể dừng và tiếp tục qua nhiều lần khởi động máy.

Central không phải agent hay harness. Nó không tự suy luận để phân rã công việc,
không gọi model trực tiếp để xây vòng lặp tool, không quản lý context nội bộ thay
Codex. Leader dùng harness để brainstorm, làm rõ yêu cầu và lập/sửa kế hoạch.
Worker dùng harness để thực hiện. Central sở hữu kế hoạch đã duyệt, lịch chạy,
trạng thái, yêu cầu phê duyệt và nguồn gốc kết quả.

MVP phục vụ cá nhân; ranh giới kiến trúc cho phép bổ sung adapter, storage và
worker từ xa sau này. Khả năng nhiều người dùng, nhiều máy hoặc HA chưa thuộc MVP.

Thành công: sau một gián đoạn, user biết việc nào đã hoàn tất, việc nào còn chưa
rõ; Central kiểm tra lại bằng chứng và tiếp tục đúng phạm vi đã duyệt, không
âm thầm chạy lại thao tác có thể tạo tác động trùng.

Thất bại dù CLI/API có vẻ hoạt động: nhận lời agent nói xong làm bằng chứng duy nhất;
mất approval sau restart; sửa kế hoạch không xin duyệt; retry mù; âm thầm đổi
model/quyền; hoặc tuyên bố resume được chỉ vì phiên agent còn tồn tại.

## 2. Quyết định đã thống nhất

- Local-first, Rust, cá nhân trước và có khả năng mở rộng.
- Giao mục tiêu bằng ngôn ngữ tự nhiên; leader làm rõ và đề xuất kế hoạch có cấu
  trúc. YAML là dạng xuất/nhập dễ đọc, không phải nơi bỏ qua validation/approval.
- Leader được đề xuất sửa kế hoạch; mọi sửa đổi phải được user duyệt mới có hiệu
  lực. Chi tiết triển khai nằm trong phạm vi tự quyết đã cấp không phải sửa kế hoạch.
- Mỗi task có cách verify, tiêu chí pass/fail, đầu vào và bằng chứng phải bàn giao.
- Mặc định verifier là agent khác, phiên riêng; user cấu hình được theo task.
- Central tự xử lý dependency và lịch chạy; không cần leader duyệt từng lượt dispatch.
- User chọn profile cho leader/worker/verifier, gồm tool, model, effort, skill và
  subagent trong khả năng thực tế adapter công bố.
- Codex là tích hợp đầu tiên. OpenCode/Muse và tính tương thích đa công cụ chưa
  thuộc nghiệm thu MVP; các thử nghiệm OpenCode được giữ làm lịch sử khảo sát.
- Khôi phục trạng thái và dữ liệu Central là trách nhiệm sản phẩm; tiếp tục nguyên
  phiên agent phụ thuộc adapter. Không được thì bàn giao sang phiên mới.
- Mất liên lạc khác thất bại: đối soát trước khi giao lại; chưa rõ thì giữ chờ,
  các nhánh độc lập vẫn tiếp tục.
- Hai vòng sửa sau lần verify đầu tiên, cấu hình được theo task; hết giới hạn
  chuyển leader. Vượt giới hạn đã duyệt cần user đồng ý.
- Profile dự phòng do user cấp trước; chuyển phải thông báo và giữ provenance.
- Một ngân sách chung cho mọi vai trò; MVP không có quỹ dự phòng.
- Chạm giới hạn: ngừng khởi chạy attempt mới, để attempt đang chạy hoàn thành.
- Usage không xác định khi run có giới hạn tiền/token: giữ chờ việc mới trong
  chính run đó đến khi đối soát được hoặc user xử lý; không tự dùng ước tính để chạy tiếp.
- Leader không ghi đè fail thành pass; bất đồng cần bằng chứng và verifier khác
  trong quyền hạn, hoặc chuyển user. Blocked không tiêu hao vòng sửa chất lượng.
- Sửa kế hoạch khi task đang chạy: để task hoàn tất theo bản đã giao rồi đánh giá
  khả năng dùng lại. Lệnh stop/thu hồi quyền trực tiếp vẫn có hiệu lực riêng.
- Workspace riêng cho mỗi attempt triển khai; tích hợp là task riêng có verify.
- Nhánh tích hợp được cập nhật từng phần; thay đổi user chỉ đưa vào khi `central sync`.
- Verify mỗi lần tích hợp, theo phần bị ảnh hưởng và nghiệm thu toàn run.
- Phiên mới kiểm tra lại workspace/bằng chứng; ghi chú agent chỉ là thông tin hỗ trợ.
- Đầu vào thay đổi: verify lại đầu ra liên quan trước, chỉ triển khai lại khi cần.
- Sửa tiêu chí verify phải giải trình tác động và xin duyệt, kể cả sửa nhỏ.
- Sau restart: tự phục hồi/đối soát thụ động, chờ `central resume` trước khi tiếp
  tục công việc agent. Không có auto-resume trong đường mặc định của MVP.
- `pause`: không giao mới, để đang chạy hoàn thành. `stop`: thêm yêu cầu hủy,
  xác nhận theo adapter; không đồng nghĩa rollback. `resume`: đối soát rồi chạy tiếp.
- Leader là đầu mối cho câu hỏi worker; vượt phạm vi thì chuyển user.
- Clean Architecture; DuckDB là lựa chọn storage dự kiến, cần qua bài thử phù hợp.
- Central không có UI đi kèm; CLI là client chính thức, dùng cùng API công khai
  với các client độc lập. Ai cũng có thể xây UI theo hợp đồng API có tài liệu.
- API mở cho tích hợp không đồng nghĩa truy cập vô danh. Client do user cấp quyền;
  MVP vẫn là một user, loopback/local, không tự mở LAN hoặc Internet.

## 3. Phạm vi sản phẩm

### Có trong MVP

CLI; daemon local; API có phiên bản dùng chung cho CLI và client độc lập; một user;
Codex adapter; nhiều profile; dependency DAG; giới hạn concurrency; task và
integration verification; kế hoạch có phiên bản; approval/question inbox;
checkpoint/bàn giao; artifact/provenance; fallback; retry có phân loại;
pause/stop/resume; budget/accounting có biểu diễn phần chưa biết.

CLI đủ vận hành toàn bộ workflow. API cung cấp run/task/attempt, timeline,
plan/revision/diff, hội thoại leader, approval/question, artifact/evidence,
profile/capability, recovery và budget. OpenAPI, schema event, hướng dẫn kết nối
và ví dụ client là đầu ra MVP; UI, frontend framework và SDK đa ngôn ngữ không
thuộc phạm vi. Client không cần đọc DB/file nội bộ hoặc suy diễn domain transition.

### Chưa có

UI chính thức hoặc host frontend, nhiều user/RBAC tổ chức, API remote qua LAN/Internet,
worker remote, hệ phân tán/HA, workflow dài nhiều tuần
tự vận hành, plugin marketplace, tự rollback tác động bên ngoài, bảo đảm exactly-once
cho mọi hành động, hay database/agent có thể thay nóng không cần migration.

### Lựa chọn của người viết, cần duyệt cùng tài liệu

- Windows x64 trước, theo môi trường đã khảo sát; giữ đường dẫn/process sau port
  để có thể bổ sung hệ điều hành khác. Chưa nghiệm thu đa nền tảng trong MVP.
- Một daemon sở hữu storage và mutation của workspace; một user OS hiện tại là
  chủ sở hữu. CLI và client độc lập không ghi trực tiếp database.
- HTTP API local dùng loopback và token local được bảo vệ bằng quyền OS; không
  mở cổng LAN. Client trình duyệt cần Origin allowlist tường minh và xác thực;
  không bật wildcard CORS hoặc cấp quyền chỉ vì request đến từ localhost.
- Không tự xóa evidence/workspace trong run chưa hoàn tất hoặc đang chờ đối soát.
  Dọn dữ liệu là thao tác tường minh; không xóa artifact còn được tham chiếu.

## 4. Các hướng kiến trúc đã cân nhắc

| Hướng | Đánh đổi | Kết luận |
| --- | --- | --- |
| CLI trực tiếp chạy agent và ghi storage | Đơn giản ban đầu, nhưng đóng CLI ảnh hưởng quản lý thực thi; khó chia sẻ với web | Không chọn |
| Daemon local, lõi use case độc lập, adapter bên ngoài | Có một nơi điều phối; thay storage/agent và thêm giao diện dễ giới hạn phạm vi | Hướng đã đồng ý |
| Coordinator/worker phân tán ngay | Hỗ trợ nhiều máy sớm, nhưng thêm lease, network và vận hành trước khi chứng minh giá trị local | Để sau |

Database đã thảo luận: SQLite là đề xuất ban đầu; user muốn DuckDB. Ta giữ DuckDB
là ứng viên, đặt sau persistence port và nghiệm thu transaction/crash/độ trễ trước
khi chốt triển khai storage. Không xây hai backend chỉ để chứng minh abstraction.

## 5. Clean Architecture và dependency

```text
CLI / HTTP handlers
         |
         v
Application use cases + ports ---> Domain
         ^
         |
DuckDB / Codex / Git / filesystem / process adapters

Composition root chọn implementation và nối dependency.
```

Domain không import SDK Codex, DuckDB, Git, web framework hoặc Tokio.
Application điều khiển nghiệp vụ và async qua port, không hiểu JSON-RPC riêng
của Codex hay câu SQL cụ thể. Adapter đổi dữ liệu ngoại vi sang kiểu của Central.
CLI/server chỉ parse/validate input ở biên và gọi use case; không lặp lại nghiệp vụ.

Cấu trúc Rust đề xuất:

```text
crates/
  central-domain/
  central-application/       # use case và port
  central-infrastructure/    # module storage, codex, git, filesystem, process
  central-cli/               # client commands và composition root CLI
  central-server/            # daemon, HTTP API, composition root server
```

Ban đầu adapter là module, chưa cần mỗi adapter một crate. Framework HTTP,
thư viện CLI và thư viện Rust database/Git để builder chọn, ghi
lý do trong implementation plan; không được làm thay đổi ranh giới trên.

### Các port có ý nghĩa nghiệp vụ

| Port | Hợp đồng cần giữ |
| --- | --- |
| Persistence / UnitOfWork | Đọc snapshot; nguyên tử cập nhật state + event + outbox với expected revision, unique command ID |
| AgentGateway | Capabilities, tạo/đọc phiên, gửi turn, sự kiện, pending request, trả lời, hủy và đối soát; không có model loop |
| WorkspaceManager | Snapshot, tạo workspace, niêm phong candidate, kiểm tra thay đổi, chuẩn bị tích hợp |
| ArtifactStore | Publish/read/hash artifact; immutable sau publish; metadata được lưu riêng |
| ProcessSupervisor | Danh tính tiến trình, liveness/ownership và yêu cầu dừng; không coi PID đơn thuần là danh tính đủ |
| Clock / Notification | Thời gian kiểm thử được; lưu/gửi cập nhật cho UI và CLI |

Tên cụ thể chưa phải API code đã chốt. Không tạo generic repository cho mọi bảng
hoặc trait giả vờ hai harness có đầy đủ cùng khả năng. Contract test kiểm tra
semantics của port, không chỉ kiểm tra cùng tên method.

## 6. Mô hình domain

| Đối tượng | Ý nghĩa |
| --- | --- |
| Run | Mục tiêu, repo, plan đang hiệu lực, điều khiển chạy, giới hạn và accounting |
| PlanRevision | Bản immutable: requirement, quyết định, DAG, phạm vi, verify, cấu hình cho phép; draft/proposed/approved/superseded |
| Task | Công việc logic trong kế hoạch; loại planning/implementation/verification/integration/assessment |
| Attempt | Một lần thực hiện task, gắn plan revision, input snapshot và cấu hình profile thực tế |
| ExecutionSegment | Một turn/đoạn thực thi trong attempt, gắn server instance/session/turn ngoại vi |
| Verification | Pass/fail/blocked, tiêu chí phiên bản, input/candidate hash, người/agent kiểm tra và bằng chứng |
| Approval | Đối tượng chính xác được duyệt, hash/revision, phạm vi quyền, actor và trạng thái |
| Question | Nội dung, bên cần trả lời, câu trả lời, tác động và correlation ngoại vi |
| Artifact | ID/hash, vị trí, loại, task/attempt/plan/input/profile tạo ra và bằng chứng verify |
| AgentProfile | Tool/model/effort/skills/subagents, quyền, fallback đã cấp và capability requirement |

Một attempt có thể có nhiều turn trong cùng phiên. Resume cùng phiên, cùng cấu
hình và đầu vào có thể tiếp tục attempt hiện tại sau đối soát. Đổi profile, retry
triển khai hoặc thay bằng phiên mới tạo attempt mới, liên kết predecessor và
bàn giao; không ghi đè lịch sử cũ.

Verification là công việc được lên lịch, có thể tiêu tốn ngân sách riêng, không
là thao tác miễn phí sau worker. Bắt đầu verifier/integrator/leader phát sinh
cũng phải qua cùng gate budget/concurrency/pause.

Không tạo chuỗi verifier kiểm tra verifier vô hạn. Hoàn tất task verification
đòi hỏi báo cáo đúng schema, đủ evidence/định danh và kết luận theo tiêu chí;
chỉ thêm reviewer khi chính sách đã duyệt hoặc có bất đồng. Planning được nghiệm
thu bằng validation cấu trúc và user duyệt nội dung, không bằng lời leader tự pass.

### Nội dung bắt buộc của task trong kế hoạch

Mục tiêu; đầu vào có định danh; phạm vi và điều không được thay đổi; dependency
thứ tự và dependency dữ liệu; output; cách verify cụ thể; tiêu chí pass/fail;
môi trường/tài nguyên; evidence; role/profile và quyền; số vòng sửa/retry; hành
động cần approval; cách đối soát và bàn giao khi có tác động khó lặp lại.

Kế hoạch còn xác định nhóm kiểm tra nền tảng, kiểm tra theo ảnh hưởng và nghiệm
thu cuối. Không được dùng prose mơ hồ như "test đầy đủ" làm tiêu chí duy nhất.

## 7. Luồng lập và sửa kế hoạch

1. User chọn leader profile và giới hạn ban đầu cho run; bước brainstorm cũng
   dùng ngân sách chung dù chưa có kế hoạch đã duyệt.
2. Leader hỏi user, khảo sát trong quyền được cấp, gửi proposal có cấu trúc.
3. Central kiểm tra schema, ID, DAG không chu trình, dependency, capability,
   tiêu chí verify và quyền; không tự đánh giá nội dung chuyên môn thay leader.
4. User xem diff/phạm vi và duyệt đúng revision/hash. Chỉnh nội dung tạo revision
   mới; approval không tự chuyển sang bản khác.
5. Central kích hoạt revision và lên lịch task đủ điều kiện.

Leader đề xuất sửa; Central trình diff, lý do, task bị ảnh hưởng và downstream.
Task bị ảnh hưởng chưa chạy bị giữ chờ. Task đang chạy được hoàn thành theo bản
cũ; kết quả gắn bản cũ và chờ assessment trước khi dùng cho bản mới. Task đã pass
có dependency dữ liệu thay đổi chuyển cần verify lại. Task độc lập tiếp tục.

Để tránh khoảng trống trong khi proposal chờ user, Central giữ chờ task chưa chạy
được proposal xác định bị ảnh hưởng và downstream; đây là cách triển khai do
người viết đề xuất. Việc chặn không cho phép thực thi nội dung proposal chưa duyệt.

Sửa tiêu chí verify phải có lý do/bằng chứng, tác động tới requirement và những
kết quả đổi pass/fail, cùng danh sách cần verify lại. Tiêu chí cũ và kết quả cũ
vẫn được giữ. Agent không tự giảm tiêu chí hoặc tự duyệt plan.

## 8. Scheduler, retry và control

Task có thể sẵn sàng khi prerequisite được nghiệm thu trên input phù hợp, plan
đã duyệt, profile/capability/quyền hợp lệ, tài nguyên không xung đột, run không
bị hold, và còn slot/budget. Dependency đúng thứ tự không đủ nếu output đã stale.

Phân biệt task logic với attempt: task triển khai chưa hoàn tất chỉ vì attempt
worker trả output; cần verification. Task tích hợp chỉ nhận candidate đã verify.
Trạng thái chính: pending/ready/running/awaitingVerification/succeeded/blocked/
failed/cancelled; biểu diễn stale và plan-held bằng lý do có kiểu, tránh suy ra
thất bại từ mất heartbeat.

Attempt có queued/dispatching/running/waitingForApproval/waitingForInput/
reconciling/unknownOutcome/completed/failed/cancelled. Kết quả chất lượng nằm ở
verification, không trộn vào transport status. Final state machine và transition
table phải được viết trong implementation plan, giữ các invariant của tài liệu.

- Chất lượng: tối đa hai vòng sửa sau lượt verify đầu; fail có evidence.
- Blocked do môi trường không tính vòng sửa chất lượng.
- Lỗi hạ tầng đã xác nhận an toàn: builder đề xuất tối đa ba retry với backoff,
  đây là default đề xuất, chưa phải quyết định riêng của user.
- Quota/auth/thiếu quyền: không retry liên tục. Chuyển fallback đã cấp nếu phù
  hợp; nếu không thì chờ user. Không tự đổi model khi không có quyền fallback.
- Unknown outcome: đối soát, không tạo execution trùng khi attempt cũ chưa rõ.
- Hết vòng sửa: yêu cầu leader phân tích khi còn budget; không vượt gate để gọi
  leader. Đổi mục tiêu/tiêu chí hoặc vượt giới hạn cần user duyệt.

`pause` đóng gate dispatch, không hủy đang chạy. `stop` đóng gate và yêu cầu hủy;
chưa xác nhận thì giữ reconciling/unknownOutcome. `resume` mở gate sau đối soát
các phần liên quan; không buộc toàn run chờ một nhánh unknown độc lập.

Trả lời một câu hỏi của attempt đang được phép chạy có thể cho nó tiếp tục; khi
stop hoặc sau restart, Central giữ câu trả lời để gửi sau explicit resume. Khi
pause/budget hold, attempt đang chạy được hoàn thành theo quyền đã duyệt, nhưng
không mở thêm verifier/retry/leader attempt mới.

## 9. Persistence và khôi phục

DuckDB dự kiến chạy embedded trong một daemon sở hữu file. Các mutation nghiệp
vụ đi qua một hàng đợi ghi; CLI/client/agent không truy cập file trực tiếp. Truy vấn
báo cáo không được làm scheduler bị block lâu. Không mở rộng ngay sang Quack hay
multi-process DB access. Kiểm tra phiên bản Rust binding và DuckDB trên Windows.

Mỗi mutation quan trọng ghi state, event với sequence và outbox intent trong
cùng transaction, có expected revision và command ID chống nhận lặp ở API.
Chỉ trả durable acknowledgement sau commit. Đây là state store + event journal,
chưa yêu cầu event sourcing/replay toàn bộ domain từ event.

Dispatch ngoại vi không nằm trong transaction DB. Outbox không tự tạo exactly-once:
ghi intent trước khi gửi; lưu server/session/turn mapping sớm nhất có thể; nếu
crash giữa gửi và ghi receipt thì chuyển ambiguous, dùng correlation và bằng
chứng để đối soát. Không tự resend một intent chưa biết đã được Codex nhận hay
chưa, trừ khi adapter chứng minh được idempotency hoặc chắc chắn chưa gửi.

Artifact publish: ghi file tạm, hoàn tất/hash/đổi tên trong store do Central quản
lý, sau đó commit reference. Crash có thể tạo orphan cần dọn, nhưng không được
coi metadata trỏ vào file thiếu là artifact hợp lệ. Đây không phải transaction
nguyên tử bao trùm filesystem và database. Database/WAL và artifact đều cần
backup nhất quán; không hứa sao chép mỗi file DB là đủ khi daemon đang ghi.

Sau restart:

1. Giành quyền sở hữu daemon/storage duy nhất; mở DB và kiểm tra phiên bản schema.
2. Đọc run/attempt/outbox; phân loại các thao tác đã commit và còn ambiguous.
3. Đối soát thụ động process, phiên và artifact khi có thể; không khởi chạy agent.
4. Khôi phục inbox từ yêu cầu còn hiệu lực; request ngoại vi cũ phải xác minh,
   không gửi lại approval accept chỉ vì DB từng ghi user đồng ý.
5. Hiển thị bản tóm tắt và chờ `central resume`.
6. Khi resume, kiểm tra workspace/evidence; dùng phiên cũ nếu an toàn, hoặc tạo
   attempt mới cùng handover đã lưu. Remote agent tương lai có thể vẫn sống;
   không giả định đóng Central là mọi agent đã dừng.

Checkpoint gồm evidence Central thu thập và ghi chú agent. Thông tin tự báo
không có bằng chứng được đánh dấu chưa xác minh. Checkpoint của workflow khác
checkpoint storage của database và không hoàn tác tác động bên ngoài.

### Gate trước khi chọn DuckDB cho implementation chính

Thử nhiều transaction nhỏ, đọc UI cùng lúc ghi, atomic state/event/outbox,
cưỡng bức dừng trước/sau commit và mở lại, chống hai daemon, lỗi dung lượng/IO.
Đo throughput, p50/p95 commit latency và số lượng task/sự kiện trong môi trường
test; builder đề xuất ngưỡng dựa trên workload MVP và nêu trước khi chạy benchmark.
Không chuyển storage âm thầm nếu không đạt; báo kết quả và cập nhật thiết kế để duyệt.

## 10. Codex adapter và profile

Ưu tiên app-server cho tương tác, vì đã quan sát được server request và replay
khi reconnect. Giữ transport bên trong adapter; không để WebSocket/JSON-RPC lan
vào application. WebSocket và một số API hiện còn experimental; pin/record CLI
version, generate schema tương ứng và kiểm tra compatibility khi kết nối.

Luồng: initialize -> tạo/resume thread -> start turn -> chuẩn hóa notification
và server request -> application xử lý -> trả lời đúng request -> xác minh item/
turn completion. Correlation gồm instance epoch + thread + turn + item + request.
ID request trong một server lifetime không được coi là ID bền vững toàn cục.

Adapter công bố model/effort/skills/subagent, status/events, input/approval,
cancellation, session continuation, structured result, usage và mức enforce
quyền. Available/unsupported/unknown phải rõ. Hỗ trợ cấu hình khác với chứng minh
được cấu hình thực tế đã áp dụng; thông tin không đọc lại được phải đánh dấu.

Profile resolution: mặc định cá nhân -> workflow -> task -> attempt override.
Snapshot cấu hình đã resolve khi dispatch; đang chạy không tự lấy cấu hình mới.
Lưu version/hash skill khi thu thập được và ghi unknown khi không thể, không
hứa đóng băng skill của harness nếu chưa có cơ chế cô lập nó.

Fallback phải đáp ứng requirement và không tăng quyền hay làm yếu giới hạn đã
chấp nhận. Thông báo lý do/profile/model/effort/bàn giao, tạo attempt mới. Cần
xác định attempt cũ đã dừng hoặc task thực sự an toàn trước khi chuyển.

Subagent bên trong Codex do Codex vận hành, không mặc nhiên có lifecycle/checkpoint
như worker Central. Công việc cần retry/verify độc lập được mô hình thành task.

Central cung cấp kênh proposal/result có schema, qua tool/API hoặc file import
được kiểm tra. Hội thoại không tự biến thành mutation. Leader chỉ có quyền đề
xuất; chỉ user được duyệt plan. Phương án wire cụ thể chọn trong plan sau spike,
không thêm vòng lặp model/tool vào Central.

## 11. Verification, workspace và tích hợp

Verifier mặc định khác phiên worker; có thể cùng tool/model. Nó nhận requirement,
candidate immutable, input, tiêu chí và evidence, không bắt buộc toàn hội thoại.
Verifier không sửa candidate hay tiêu chí để đạt; test ghi vào scratch/output
riêng. Tests có thể cần thư mục ghi; read-only source không đồng nghĩa mọi test
chạy được trong read-only filesystem toàn phần.

Pass/fail/blocked gắn chính xác plan/criteria/input/candidate/environment. Leader
có thể yêu cầu reviewer khác trong giới hạn; không ghi đè fail. User có thể sửa
tiêu chí qua revision đã duyệt; bypass thủ công nếu được bổ sung sau này phải có
trạng thái riêng, không giả thành verification pass.

Run có nhánh tích hợp do Central quản lý; implementation attempt có worktree
riêng ở input snapshot được ghi nhận. Integration task dùng workspace ứng viên,
ghép candidate rồi chạy kiểm tra. Chỉ cập nhật tích hợp sau pass; candidate lỗi
giữ lại, nhánh tốt trước đó vẫn còn. Đây là chuỗi integration task có thứ tự,
không cho nhiều agent cùng sửa worktree tích hợp.

Task đang chạy không bị rebase dưới chân. Task mới dùng input mới nhất phù hợp
dependency, không chỉ commit HEAD mới nhất bất kể hợp đồng đầu vào.

`central sync` nhận commit user chọn, đưa vào hàng đợi tích hợp; không tự sửa
workspace user hoặc hút mọi thay đổi. Đề xuất MVP yêu cầu chọn commit, còn thay
đổi chưa commit cần user tạo snapshot tường minh hoặc commit trước; không tự stash.
Chốt input run cũng phải chỉ rõ commit hoặc snapshot thay đổi đã chọn.

Ảnh hưởng ngữ nghĩa do leader đề xuất. Central lan truyền invalidation theo
dependency dữ liệu và các input version; không suy luận rằng khác file là an toàn.
Lịch chạy còn xét tài nguyên dùng chung như port/database test, không chỉ Git.

## 12. Approval, câu hỏi và quyền

Phân biệt: duyệt kế hoạch/revision; duyệt thao tác harness; quyết định vận hành
như tăng giới hạn; và câu hỏi làm rõ. Mỗi quyết định có actor, target/hash,
scope, timestamp và trạng thái hiệu lực. Nội dung thay đổi phải xét duyệt lại.

Leader trả lời câu hỏi worker trong requirement/phạm vi đã duyệt. Yêu cầu mới
chuyển user; không tự biến câu trả lời thành sửa kế hoạch có hiệu lực. Central
lưu câu trả lời và bàn giao được qua phiên mới.

Pending request ngoại vi có vòng đời riêng: replay khi reconnect, resolved,
interrupted, expired hoặc stale khi server đổi epoch. Không dùng lại accept cũ
cho request mới dù text tương tự. Nếu approval không sống qua restart, Central
hiển thị cần đối soát và tái yêu cầu khi adapter xác nhận cần; không tự cho chạy.

Mức enforce: enforced / instruction-only / unsupported. User có thể chấp nhận
instruction-only một cách tường minh; ghi nhận theo profile/run. Không tuyên bố
workspace là sandbox, không cho fallback âm thầm hạ mức bảo vệ.

## 13. Budget và observability

Một budget chung bao gồm lập kế hoạch, leader xử lý phát sinh, implementation,
verification và integration. Giới hạn concurrency/attempt count và dispatch gate
được enforce nội bộ. Retry/fallback không được né giới hạn. Khi chạm ngưỡng,
đang chạy được hoàn thành; số liệu cuối có thể vượt ngưỡng.

Phân biệt observed/estimated/unknown usage. Không cộng các cumulative event lặp
lại; gắn usage đúng provider/session/turn và phiên bản cách diễn giải. Lượt crash
có thể thiếu số liệu cuối. Không suy ra giá tiền chỉ từ token hoặc coi quota
ChatGPT tương đương tiền API. Giới hạn tiền chỉ là ngưỡng dispatch theo telemetry/
ước tính được công bố, không hard cap chi tiêu nếu adapter không bảo đảm được.

**Chính sách đã được user chốt sau builder review:** nếu run có giới hạn tiền/
token và usage bị thiếu hoặc không xác định, Central đóng gate giao việc mới
của run đó đến khi đối soát được hoặc user xử lý tường minh. Không tự suy diễn
phần chưa biết bằng 0, không tự chuyển sang ước tính để tiếp tục. Hold áp dụng
cho worker, verifier, integrator, leader phát sinh, retry và fallback mới;
attempt đang chạy vẫn được hoàn thành. Các run khác không bị hold theo.

Đối soát không gọi model và thao tác đọc/xem trạng thái vẫn được thực hiện. Nếu
cần gọi leader để xử lý thì phải chờ gate được mở. CLI/API cung cấp phần usage thiếu,
nguồn bằng chứng và lý do hold. User có thể giải quyết cách ghi nhận phần thiếu
hoặc điều chỉnh chính sách ngân sách một cách tường minh, có audit; không sửa
số liệu ước tính thành observed. `central resume` đơn thuần không xóa hold này.

CLI/API cung cấp trạng thái có lý do, phiên bản kế hoạch, attempt/profile/model,
chờ ai, evidence, mức xác minh và accounting chưa biết. Thông báo fallback,
unknown outcome, cần user và hoàn thành; không lặp lại thông báo không đổi.
Không ghi credentials vào journal/artifact; secret answer không được lưu plaintext
trong trace thông thường. Chính sách secret persistence cần chọn trước khi hỗ
trợ loại câu hỏi đó; MVP có thể yêu cầu nhập lại thay vì lưu.

## 14. CLI và API cho client độc lập

### Ranh giới sản phẩm

Quyết định ngày 2026-11-10 thay hướng văn phòng pixel trước đây: Central phát hành
daemon và CLI, không frontend, static assets hoặc lệnh `central ui`. Mockup ở
`docs/superpowers/mockups/` là lịch sử, có thể tham khảo cho UI độc lập sau này.
Client tự chọn cách trình bày; Central không quy định layout, nhân vật hay panel.

CLI là một consumer của cùng API mà client ngoài sử dụng. Domain/application giữ
toàn bộ scheduling, approval, budget và verification; không có thao tác chỉ làm
được bằng cách sửa DB/file nội bộ hoặc chạy business logic trong client.

### Hợp đồng API

- HTTP JSON có version `/v1`, OpenAPI có thể tải và schema event có tài liệu.
  DTO công khai tách khỏi snapshot lưu trữ và Codex wire protocol; không để thay
  đổi Rust struct nội bộ vô tình thành breaking change cho client.
- Đọc đủ run/task/attempt, plan/revision/diff, hội thoại, inbox, artifact/evidence,
  profile/capability, usage và recovery. Danh sách/log/history có phân trang.
- Các lệnh ghi dùng cùng application use case với CLI, command ID chống lặp và
  revision/hash/epoch phù hợp. Receipt xác nhận commit lệnh; kết quả task đến sau.
  Client khác đang giữ dữ liệu cũ nhận conflict, không tự cấp lại approval.
- Snapshot nhất quán kèm cursor event bền vững; SSE và đọc event theo cursor giúp
  nối lại, phát hiện khoảng thiếu và tải snapshot mới. Đổi daemon instance cần
  đồng bộ lại trạng thái recovery; reconnect không tự resume hoặc trả lời inbox.
- User cấp/thu hồi credential cho client với quyền đọc hoặc điều khiển. Credential
  owner, client và agent bridge không hoán đổi quyền; actor được suy ra ở server.
  Origin allowlist cho browser client là cấu hình user, không là cơ chế xác thực.
- Browser example dùng `fetch` và đọc stream với Authorization header; không đưa
  token vào URL. Không buộc client dùng cookie hoặc UI do Central host.
- Tài liệu nêu rõ errors, idempotency, pagination, event replay, tương thích phiên
  bản và capability chưa hỗ trợ. Không hứa exactly-once cho tác động bên ngoài.

### Tiêu chí nghiệm thu client

Một client ví dụ nhỏ chỉ dựa trên API được công bố phải đọc trạng thái, gửi lệnh,
duyệt đúng revision, xử lý inbox và reconnect được; không import code nội bộ.
CLI và client độc lập quan sát cùng trạng thái và cùng lỗi khi stale/không đủ quyền.
Luồng worker -> verifier -> integration phải chạy qua daemon thật; fake test vẫn
ghi rõ giới hạn, không thay bằng chứng live-model. Không cần xây một UI để nghiệm thu.

### CLI

Các tên lệnh là đề xuất giao diện, builder có thể chỉnh trước khi chốt CLI spec:

```text
central init
central start "Migrate module A, giữ tương thích API"
central status [run]
central plan show|diff|approve ...
central inbox
central approve|decline ...
central answer ...
central chat ...
central profiles ...
central clients ...
central events ...
central logs ...
central artifacts ...
central sync ...
central pause|stop|resume ...
```

Đóng CLI không đóng daemon. `start` chưa duyệt kế hoạch không cho implementation
chạy. CLI có output máy đọc được cho automation. Agent không có endpoint tự approve plan.
User auth và agent capability token cần tách quyền tại API dù chỉ có một user.

## 15. Bằng chứng đã có và giới hạn

Các đường dẫn dưới đây tương đối với root repository:

- `spikes/codex-lifecycle/FINDINGS.md`: gọi thật và resume sau lượt hoàn tất.
- `spikes/codex-lifecycle/CRASH-FINDINGS.md`: ngắt giữa tool, checkpoint còn;
  resume cùng session, agent inspect rồi gọi recovery được chuẩn bị sẵn.
- `spikes/codex-app-server/FINDINGS.md`: real approval và structured question,
  trạng thái chờ, replay request sau thay kết nối WebSocket, trả lời rồi hoàn tất.

Đã dùng model nhỏ gpt-6-luna/low cho các thử nghiệm sau theo yêu cầu tiết kiệm.
Model nhỏ không tự giảm input context; thử nghiệm có token telemetry nhưng
không đầy đủ accounting cho lượt bị ngắt.

Chưa chứng minh: Central restart (chưa có Central), pending request qua app-server
restart, reboot/power loss, file/permission approval và deny/cancel paths, profile
cách ly, đồng thời nhiều worker, kỹ năng/subagent, DuckDB workload, cũng như
phục hồi tác động bên ngoài tổng quát. Không nâng kết quả spike thành bảo đảm.

## 16. Nghiệm thu và thứ tự triển khai đề xuất

1. **Storage gate:** spike DuckDB nêu ở mục 9; quyết định dựa trên số liệu.
2. **Lát cắt đầu:** user duyệt plan, worker nhỏ, verifier phiên riêng, artifact
   và evidence; daemon bị ngắt rồi restart phải chờ explicit resume.
3. **Recovery:** injection ở trước/sau commit, dispatch không rõ kết quả,
   agent đang chờ input/approval, candidate chưa ghi receipt; không giao trùng mù.
4. **Điều phối:** hai implementation task độc lập, dependency dữ liệu, tài nguyên
   xung đột, profile khác nhau, fail->hai vòng sửa->leader, blocked riêng biệt.
5. **Revision/tích hợp:** user sync commit, sửa kế hoạch khi đang chạy, tái sử dụng
   candidate cũ qua assessment, invalidation và ba mức verification.
6. **Vận hành:** pause/stop/resume, quota/fallback có thông báo, budget gate, usage
   unknown giữ chờ đúng run và chỉ mở khi đối soát/user xử lý; CLI và client ngoài
   điều khiển inbox qua cùng API/application use case, nối lại event sau restart.

Kiểm thử domain bằng transition/invariant; application bằng port fake có failure
injection; adapter bằng contract/integration test; end-to-end dùng một repo Git
fixture nhỏ, không migration dự án thật. Không gọi model thật cho mọi unit test.
Real-model checks có chủ đích, model nhỏ và giới hạn rõ; không hứa token hard cap.

Nghiệm thu cuối: cho nhiều task tạo thay đổi nhỏ trên fixture, verify độc lập,
ngắt giữa chừng, restart, user resume, tích hợp và nghiệm thu; truy được plan,
input, attempt, profile và bằng chứng của mọi đầu ra. Nhánh unknown không bị tự
giao lại; plan chưa duyệt không được thi hành; không có lời tự báo xong thay verify.

## 17. Các quyết định để builder xử lý trong plan

Chọn thư viện và version phù hợp, schema SQL/index/migration, bảng transition
đầy đủ, định dạng wire plan/result và công cụ bridge cho Codex, cách supervisor
quản lý process identity/Windows sandbox, batching telemetry, API DTO/schema,
API endpoint names và measured workload thresholds. Các lựa chọn phải giữ nguyên
quyền user, transaction semantics, capability honesty và gate khôi phục đã nêu.

Nếu DuckDB không đạt hoặc Codex version không hỗ trợ chức năng bắt buộc, báo rõ
bằng chứng và đề xuất sửa thiết kế; không che giới hạn bằng parser đoán hội thoại.

### Kết quả builder review

Reviewer nêu một câu hỏi về budget gate khi usage không xác định. User đã chọn
giữ chờ việc mới trong run đó cho đến khi đối soát được hoặc user xử lý. Quy tắc
được đưa vào mục 2, mục 13 và nghiệm thu mục 16. Không còn câu hỏi sản phẩm đang
chờ trả lời từ vòng review này. Sau vòng UI, user yêu cầu viết implementation
plan trên baseline này. Ngày 2026-11-10 user thay phạm vi bằng CLI và API cho client
độc lập; P4 có một task, không xây UI. Các lựa chọn triển khai của người viết sẽ
xuất hiện trong plan để review; yêu cầu viết plan không tự cho phép bắt đầu code.

## 18. Nguồn kỹ thuật

- Codex protocol: https://learn.chatgpt.com/docs/app-server
- DuckDB concurrency: https://duckdb.org/docs/2.0/connect/concurrency
- DuckDB transactions: https://duckdb.org/docs/2.0/sql/statements/transactions
- DuckDB crash recovery: https://duckdb.org/docs/2.0/guides/troubleshooting/crashes
- OpenAPI: https://spec.openapis.org/oas/latest.html
- SSE: https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
- Fetch/Authorization: https://developer.mozilla.org/en-US/docs/Web/API/Fetch_API/Using_Fetch

Nguồn online được khảo sát trong cuộc thảo luận; implementation cần pin version
thực tế và dùng schema/docs tương ứng. Các spike trong repository là bằng chứng
quan sát, không thay thế kiểm thử adapter sản phẩm.
