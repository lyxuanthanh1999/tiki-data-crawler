import html
import re
from typing import Any, Dict, List, Optional


def clean_description(raw_html: Optional[str]) -> str:
    """
    Chuẩn hoá nội dung description:
    1. Loại bỏ các thẻ script/style và nội dung của chúng.
    2. Chuyển đổi các thẻ khối (p, div, li, h1-h6, br,...) thành dấu xuống dòng hợp lý.
    3. Loại bỏ toàn bộ thẻ HTML còn lại.
    4. Unescape các ký tự HTML entities (&nbsp;, &amp;, &quot;, &lt;, &gt;, &#...;).
    5. Xoá các khoảng trắng đặc biệt (\xa0, zero-width space).
    6. Chuẩn hoá khoảng trắng dư thừa trong từng dòng và giữa các đoạn.
    """
    if not raw_html or not isinstance(raw_html, str):
        return ""

    text = raw_html

    # 1. Loại bỏ thẻ script và style cùng nội dung bên trong
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", text, flags=re.DOTALL | re.IGNORECASE)

    # 2. Thay thế thẻ khối và thẻ ngắt dòng bằng ký tự newline (\n)
    text = re.sub(
        r"</?(?:p|div|li|h[1-6]|tr|table|ul|ol|blockquote|section|article|header|footer)[^>]*>",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)

    # 3. Loại bỏ tất cả các thẻ HTML còn lại (span, b, i, strong, a, v.v.)
    text = re.sub(r"<[^>]+>", " ", text)

    # 4. Giải mã các HTML entities (ví dụ: &nbsp; -> khoảng trắng, &amp; -> &)
    text = html.unescape(text)

    # 5. Thay thế ký tự khoảng trắng đặc biệt
    text = text.replace("\xa0", " ").replace("\u200b", "").replace("\r", "")

    # 6. Chuẩn hoá khoảng trắng trên từng dòng
    lines = [re.sub(r"[ \t]+", " ", line).strip() for line in text.splitlines()]

    # Ghép lại các dòng, giữ tối đa 1 dòng trống phân cách giữa các đoạn
    cleaned_lines: List[str] = []
    prev_empty = False
    for line in lines:
        if line:
            cleaned_lines.append(line)
            prev_empty = False
        elif not prev_empty:
            cleaned_lines.append("")
            prev_empty = True

    return "\n".join(cleaned_lines).strip()


def extract_product_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Trích xuất các thông tin cần thiết từ API response của Tiki:
    - id: Mã sản phẩm
    - name: Tên sản phẩm
    - url_key: Đường dẫn URL thân thiện
    - price: Giá bán
    - description: Nội dung mô tả đã được làm sạch và chuẩn hoá
    - images: Danh sách các URL hình ảnh sản phẩm
    """
    if not isinstance(data, dict):
        return {}

    # Trích xuất danh sách link hình ảnh (ưu tiên base_url, sau đó đến url / medium_url / large_url)
    raw_images = data.get("images", [])
    image_urls: List[str] = []
    if isinstance(raw_images, list):
        for img in raw_images:
            if isinstance(img, dict):
                url = img.get("base_url") or img.get("url") or img.get("large_url") or img.get("medium_url")
                if url and isinstance(url, str):
                    image_urls.append(url)
            elif isinstance(img, str) and img.startswith("http"):
                image_urls.append(img)

    # Nếu images rỗng, thử lấy từ thumbnail_url
    if not image_urls and data.get("thumbnail_url"):
        image_urls.append(data["thumbnail_url"])

    raw_description = data.get("description")
    # Nếu description rỗng, thử fallback lấy short_description
    if not raw_description:
        raw_description = data.get("short_description")

    return {
        "id": data.get("id"),
        "name": data.get("name"),
        "url_key": data.get("url_key"),
        "price": data.get("price"),
        "description": clean_description(raw_description),
        "images": image_urls,
    }
