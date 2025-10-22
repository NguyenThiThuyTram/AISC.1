import requests
import json
import base64
from collections import defaultdict

# --- CẤU HÌNH API GITHUB ---
GITHUB_USERNAME = "octocat"
GITHUB_TOKEN = "YOUR_PERSONAL_ACCESS_TOKEN" 

HEADERS = {
    'Authorization': f'token {GITHUB_TOKEN}',
    'Accept': 'application/vnd.github.v3+json'
}
BASE_API_URL = "https://api.github.com"

def get_repo_languages(owner: str, repo_name: str) -> dict:
    """
    Lấy dữ liệu Ngôn ngữ (theo Byte) từ một Repository cụ thể.
    """
    url = f"{BASE_API_URL}/repos/{owner}/{repo_name}/languages"
    # print(f"-> Gọi API: {url}")
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()  # Xử lý lỗi HTTP (4xx, 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"LỖI INGEST API LANGUAGES: {e}")
        return {}

def get_readme_content(owner: str, repo_name: str) -> str:
    """
    Lấy nội dung (content) của file README và giải mã base64.
    """
    readme_url = f"{BASE_API_URL}/repos/{owner}/{repo_name}/readme"
    try:
        readme_response = requests.get(readme_url, headers=HEADERS)
        readme_response.raise_for_status()
        readme_data = readme_response.json()
        
        content_base64 = readme_data.get('content', '')
        if content_base64:
            # Giải mã Base64 thành chuỗi
            # Tham số validate=True được thêm vào base64.b64decode() nếu dùng Python 3.12+
            content_decoded = base64.b64decode(content_base64, validate=True).decode('utf-8')
            return content_decoded
        return ""
        
    except requests.exceptions.RequestException as e:
        # README có thể không tồn tại (404), chỉ in lỗi nếu cần thiết
        # print(f"LỖI INGEST README: {e}") 
        return ""

# HÀM ĐƯỢC SỬA LỖI: Tích hợp gọi README
def get_repo_details(owner: str, repo_name: str) -> dict:
    """
    Lấy Mô tả, Chủ đề (Topics), Tên Repo và nội dung README.
    """
    url = f"{BASE_API_URL}/repos/{owner}/{repo_name}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        details = response.json()
        
        # Lấy Topics (thường là một lệnh API riêng)
        topics_url = f"{url}/topics"
        topics_response = requests.get(topics_url, headers=HEADERS)
        topics_response.raise_for_status()
        topics_data = topics_response.json().get('names', [])
        
        # Bổ sung: Gọi hàm lấy README
        readme_content = get_readme_content(owner, repo_name)

        return {
            "name": details.get("name"),
            "description": details.get("description", ""),
            "topics": topics_data,
            "readme": readme_content, # THÊM NỘI DUNG README VÀO RAW DATA
            "html_url": details.get("html_url")
        }
    except requests.exceptions.RequestException as e:
        print(f"LỖI INGEST CHI TIẾT REPO: {e}")
        return {}

if __name__ == '__main__':
    REPO_OWNER = "octocat"
    REPO_NAME = "Spoon-Knife"
    
    print("--- CHẠY THỬ INGEST GITHUB CLIENT ---")
    
    # Lấy dữ liệu ngôn ngữ
    raw_languages_data = get_repo_languages(REPO_OWNER, REPO_NAME)
    
    # Lấy chi tiết repo (bao gồm README)
    raw_details_data = get_repo_details(REPO_OWNER, REPO_NAME)

    print("\n[KẾT QUẢ THU THẬP DỮ LIỆU THÔ]")
    print(f"Ngôn ngữ: {raw_languages_data}")
    print(f"Chi tiết Repo: {json.dumps(raw_details_data, indent=2)[:500]}...")
    print(f"Độ dài README: {len(raw_details_data.get('readme', ''))} ký tự")