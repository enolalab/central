# Chat với Leader: dễ đọc, có hồi kết, chốt rõ ràng — Thiết kế để duyệt

Trạng thái: **User đã duyệt 2026-11-26**. Plan: `docs/superpowers/plans/2026-11-26-leader-chat-convergence.md`.
Ngày: 2026-11-26, múi giờ Asia/Bangkok.

## 1. Bối cảnh và bằng chứng

User phản ánh ba vấn đề khi chat với leader trên web app:

1. Khó đọc vì tin nhắn chứa ký tự format (`**`, backtick) nhưng khung chat hiển
   thị thuần text.
2. Không rõ ranh giới giữa thảo luận và chốt kế hoạch.
3. Leader chat không có hồi kết, đi lòng vòng mà không ra kế hoạch.

Chẩn đoán dựa trên một cuộc chat thật (9 lượt, chủ đề "nâng cao trí tuệ BOT")
và đọc mã. Đây là một mẫu duy nhất nhưng các mẫu lặp lại nhất quán.

| Quan sát trong cuộc chat | Nguyên nhân trong mã |
|---|---|
| `**...**` và backtick hiện thô ở mọi tin | `renderMessage` chỉ tách đoạn rồi gán `textContent` (`crates/central-server/web/workspace.js`) |
| 9 lượt, không có kế hoạch nào | Prompt chỉ cho đề xuất khi "user đã đồng ý" hoặc "yêu cầu rõ" (`crates/central-application/src/discussion.rs`). User đang khám phá nên không bao giờ nói "lên kế hoạch" |
| Mỗi tin kết thúc bằng "Bạn muốn X hay Y?", user bỏ qua | Prompt khuyến khích "explore options across multiple messages", không yêu cầu đề xuất hành động |
| "đào sâu đi" → nội dung gần như lặp lại | Prompt không yêu cầu tránh lặp hay tự chọn khi user không chọn |
| Câu rào đón lặp lại ở hầu hết đoạn | Không có yêu cầu ngắn gọn |
| Lúc thì leader đọc được mã, lúc bảo user dán mã | `workspace_read_only = local_checkout && provider == "codex"` (`crates/central-server/src/http/chat.rs`). User không thấy điều kiện này trên UI |
| Kế hoạch (nếu có) chỉ là thẻ "↗" nhỏ; modal chỉ có "Duyệt và chạy" | `showPlan` không có đường yêu cầu sửa |

Đã bác bỏ sau khi xem bằng chứng: leader hỏi lại câu đã hỏi, quá nhiều câu hỏi
mỗi lượt. Vì vậy thẻ câu hỏi, `decided[]` và bộ đếm lượt **không** thuộc thiết
kế này.

## 2. Mục tiêu và ngoài phạm vi

Mục tiêu:

- Tin nhắn leader hiển thị markdown đã render, an toàn.
- User luôn thấy lối ra: một nút lập kế hoạch do user bấm.
- Leader kết thúc mỗi tin bằng một đề xuất hành động cụ thể, không mở nhánh mới vô hạn.
- Kế hoạch đang chờ có thể duyệt hoặc yêu cầu sửa.
- User biết leader có đọc được mã workspace hay không.

Ngoài phạm vi:

- Đổi schema `LeaderReplyV1` hoặc `PlanDocumentV1`.
- Thẻ câu hỏi, danh sách quyết định do leader tự khai, bộ đếm lượt.
- Bảng Brief cố định cạnh chat (có thể xét sau).
- Mở rộng quyền đọc workspace cho provider khác Codex.

Bất biến giữ nguyên: chat không bao giờ đổi kế hoạch; đề xuất chỉ thành
*pending*; chỉ user duyệt theo hash.

## 3. Quyết định đã chốt với user

| # | Câu hỏi | Quyết định |
|---|---|---|
| 1 | Leader có tự đưa kế hoạch khi chưa được yêu cầu? | **Không.** Leader chỉ gợi ý "đã đủ để lập kế hoạch cho X"; user bấm nút |
| 2 | Sau khi user bấm "Lập kế hoạch", leader có được hỏi? | **Được hỏi đến khi hiểu rõ kế hoạch**, có rào chắn ở §4.3 |
| 3 | "Yêu cầu sửa" hoạt động thế nào? | User nhập góp ý, leader đề xuất bản mới thay bản đang chờ |
| 4 | Render markdown bằng gì? | Nhúng thư viện: marked + DOMPurify |

## 4. Thiết kế

### 4.1 Ý định của lượt (`intent`)

Mỗi lượt leader mang một ý định:

- `discuss`: mặc định, như hiện nay.
- `plan`: user đã bấm "Lập kế hoạch".
- `revise`: user yêu cầu sửa kế hoạch đang chờ, kèm góp ý.

Thay đổi:

- API: `PostChatBody` thêm `intent` tùy chọn (mặc định `discuss`) cho endpoint
  `leader-chat`. Giá trị lạ trả 422.
- Domain: `LeaderDiscussion` thêm `intent` với `#[serde(default)]` để đọc được
  event cũ. Không thêm lệnh domain mới.
- `revise` chỉ hợp lệ khi có `pending_plan`; `plan`/`revise` bị từ chối khi đã có
  `active_plan` (giữ quy tắc hiện tại: không thay kế hoạch đã duyệt qua chat).
- Chế độ lập kế hoạch được suy ra từ lượt gần nhất: nếu lượt cuối là `plan`
  hoặc `revise` và chưa tạo đề xuất, ô nhập tiếp tục gửi cùng `intent` đó.
  Khi đề xuất được tạo, quay về `discuss`. User có thể thoát chế độ bằng nút
  "Quay lại thảo luận".

`ProposePlan` đã tự đánh dấu bản đang chờ cũ là `Superseded` khi có bản mới
(`crates/central-domain/src/decision.rs`), nên "Yêu cầu sửa" không cần lệnh mới.

### 4.2 Prompt cho lượt `discuss`

Sửa prompt trong `discussion::prompt`:

- Trả lời ngắn gọn, nêu điều quan trọng trước. Rào đón (giới hạn của bằng chứng)
  nói **một lần** ở cuối nếu cần, không lặp ở từng đoạn.
- Được dùng markdown nhẹ: tiêu đề, danh sách, đậm, code, bảng.
- Không lặp lại nội dung đã nói ở lượt trước; chỉ thêm điều mới.
- Nếu đã đưa lựa chọn mà user không chọn (ví dụ "đào sâu đi"), tự chọn phương
  án hợp lý nhất và nói rõ đã chọn gì.
- Kết thúc tin bằng **một bước tiếp theo cụ thể**, không phải câu hỏi sở thích.
  Khi đã có việc làm được, câu kết là gợi ý dạng:
  "Đã đủ để lập kế hoạch cho: <việc>. Bấm **Lập kế hoạch** khi bạn sẵn sàng."
- Chỉ hỏi khi câu trả lời thay đổi đáng kể hướng đi, và luôn kèm phương án mặc định.
- Không đề xuất kế hoạch trong lượt `discuss` (quyết định 1). Bỏ hướng dẫn JSON
  khỏi prompt `discuss`; nếu model vẫn trả JSON có `proposal`, server bỏ qua
  `proposal` và chỉ giữ `message`.
- Nếu `canInspect` là false và user nhờ đọc mã: nói rõ phiên này không đọc được
  workspace và lý do (provider/checkout), thay vì chỉ xin user dán mã.

### 4.3 Prompt cho lượt `plan` và rào chắn

Prompt `plan` yêu cầu leader dựa trên toàn bộ cuộc trao đổi để lập kế hoạch cho
mục tiêu user vừa nêu (hoặc gợi ý gần nhất của leader nếu user không ghi gì).

Leader được hỏi tiếp (quyết định 2), nhưng phải tuân thủ:

1. Chỉ hỏi câu **chặn**: không có câu trả lời thì không viết được task hoặc
   tiêu chí kiểm chứng. Câu không chặn thì tự giả định và ghi ra.
2. Mỗi lượt tối đa 3 câu, đánh số, mỗi câu kèm phương án mặc định.
3. Không mở thêm phương án hay chủ đề mới; không phân tích lại.
4. Không hỏi lại điều user đã trả lời trong cuộc trao đổi.
5. Khi không còn câu chặn, trả về JSON đề xuất (định dạng hiện tại) với
   `message` tóm tắt kế hoạch và liệt kê **giả định**.

UI ở chế độ này:

- Thanh trạng thái ghi "Đang lập kế hoạch: leader có thể hỏi các câu cần thiết".
- Nút **"Lập ngay với giả định"**: gửi lượt `plan` có chỉ thị cấm hỏi, buộc
  leader xuất kế hoạch và ghi mọi điểm chưa rõ thành giả định. Đây là lối thoát
  khi việc hỏi bắt đầu lòng vòng.

### 4.4 Prompt cho lượt `revise`

Đầu vào gồm kế hoạch đang chờ (document) và góp ý của user. Leader trả về kế hoạch
sửa theo định dạng JSON hiện tại, với `message` nêu các điểm đã đổi. Rào chắn hỏi
như §4.3. Đề xuất mới thay bản đang chờ qua `ProposePlan`.

### 4.5 Giao diện

- **Markdown**: tin nhắn leader đã hoàn tất được render bằng marked rồi lọc qua
  DOMPurify. Tin nhắn của user và bản xem trước khi đang stream vẫn hiển thị
  thuần text, để tránh JSON hoặc markdown dở dang bị render sai.
- **Nút "Lập kế hoạch"** cạnh nút Gửi, hiện khi đã có ít nhất một lượt thảo luận,
  chưa có `active_plan` và không có lượt đang chạy. Bấm vào thì nội dung ô nhập
  (có thể trống) được gửi với `intent: plan`.
- **Kế hoạch đang chờ**: hiển thị nổi bật ngay dưới tin nhắn tạo ra nó (mục tiêu,
  số task, nút "Xem chi tiết"), không chỉ là thẻ "↗". Thanh trên cùng ghi
  "Có kế hoạch chờ duyệt".
- **Modal kế hoạch**: thêm nút **"Yêu cầu sửa"** cạnh "Duyệt và chạy". Bấm mở ô
  nhập góp ý, gửi với `intent: revise`.
- **Chỉ báo quyền đọc mã**: tiêu đề chat hiện "Đọc được mã workspace" hoặc
  "Không đọc được mã workspace" kèm lý do ngắn. Server đưa giá trị dự kiến cho
  lượt tiếp theo vào read model; dữ liệu theo từng lượt đã có trong
  `LeaderDiscussion.workspace_read_only`.

### 4.6 Nhúng marked và DOMPurify

- Thêm file đã build sẵn vào `crates/central-server/web/vendor/` cùng tệp license
  (marked: MIT; DOMPurify: Apache-2.0/MPL-2.0). Ghi phiên bản cố định trong
  `vendor/README.md`.
- Phục vụ qua route `/app/vendor/*.js` bằng `include_str!` như các asset hiện có.
  CSP hiện tại (`script-src 'self'`) giữ nguyên.
- Cấu hình: tắt HTML thô (DOMPurify chỉ cho danh sách thẻ an toàn), bỏ `img`,
  link mở với `rel="noopener noreferrer"` và `target="_blank"`.
- Nếu thư viện không tải được, rơi về hiển thị thuần text như hiện nay.

## 5. Trường hợp lỗi

| Tình huống | Xử lý |
|---|---|
| Lượt `plan` trả JSON nhưng kế hoạch không hợp lệ | Lượt báo lỗi như hiện tại; vẫn ở chế độ lập kế hoạch; UI cho gửi lại hoặc "Lập ngay với giả định" |
| Lượt `plan` trả text thường | Coi là leader đang hỏi; vẫn ở chế độ lập kế hoạch |
| Lượt `discuss` trả JSON có `proposal` | Bỏ `proposal`, hiển thị `message` |
| `revise` khi không còn `pending_plan` (ví dụ đã duyệt ở tab khác) | 409; UI tải lại và báo "Kế hoạch đã thay đổi" |
| Markdown chứa HTML hoặc script | DOMPurify loại bỏ; có test |

## 6. Tiêu chí chấp nhận

1. Tin nhắn leader có `**đậm**`, danh sách, code, bảng được render đúng; không còn
   ký tự format thô. HTML/script trong tin nhắn không thực thi (test).
2. Ở lượt `discuss`, server không tạo `pending_plan` dù model trả JSON đề xuất (test).
3. Bấm "Lập kế hoạch" → các lượt mang `intent: plan` cho đến khi có `pending_plan`;
   sau đó quay về `discuss` (test domain và UI).
4. "Lập ngay với giả định" tạo đề xuất trong một lượt khi model tuân thủ (kiểm tra
   live với Codex).
5. "Yêu cầu sửa" tạo bản mới; bản cũ có trạng thái `Superseded` (test).
6. Event cũ không có `intent` vẫn đọc được và được coi là `discuss` (test).
7. Chỉ báo quyền đọc mã khớp với `workspace_read_only` của lượt tiếp theo.
8. Phát lại cuộc chat "trí tuệ BOT" với prompt mới: leader kết thúc tin bằng bước
   tiếp theo cụ thể, không lặp nội dung khi user nói "đào sâu đi" (đánh giá thủ công).

## 7. Rủi ro và câu hỏi mở

- **Quyết định 2 có thể làm vòng hỏi kéo dài.** Giảm rủi ro bằng rào chắn §4.3 và
  nút "Lập ngay với giả định". Nếu thực tế vẫn lòng vòng, cân nhắc giới hạn số lượt
  hỏi trong chế độ lập kế hoạch.
- **Prompt là ràng buộc mềm.** Model có thể vẫn kết thúc bằng câu hỏi sở thích.
  Chỉ đo được qua dùng thật; tiêu chí 8 là kiểm tra thủ công.
- **Hành vi khác nhau giữa provider** (Codex, Muse, opencode) khi trả JSON. Kiểm tra
  live ít nhất với Codex; provider khác ghi nhận là chưa xác minh.
- **Thêm ~60 KB JS vendor** vào binary. Chấp nhận được với app local.
- Phạm vi bảng Brief cố định để lại sau khi có dữ liệu dùng thật.

## 8. Thứ tự triển khai đề xuất

1. Markdown (vendor + render + fallback). Chỉ UI, rủi ro thấp, thấy hiệu quả ngay.
2. Sửa prompt `discuss` (§4.2) và bỏ qua `proposal` ở lượt `discuss`.
3. `intent` trong API/domain, prompt `plan`, nút "Lập kế hoạch", "Lập ngay với giả định".
4. Hiển thị kế hoạch nổi bật, "Yêu cầu sửa" với `intent: revise`.
5. Chỉ báo quyền đọc mã.

Mỗi bước có thể dùng thử độc lập; bước 1–2 đã giải quyết phần lớn vấn đề 1 và 3.
