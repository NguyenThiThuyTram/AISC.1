import spacy
from spacy.pipeline import EntityRuler
from collections import defaultdict
import json 
# LƯU Ý: Không import requests, base64, hay GITHUB_TOKEN ở đây!

# --- CƠ SỞ TRI THỨC ĐƠN GIẢN (TAXONOMY TĨNH) ---
CUSTOM_SKILL_PATTERNS = [
    {"label": "DOMAIN", "pattern": [{"LOWER": "fintech"}]},
    # ... (Các pattern khác giữ nguyên) ...
    {"label": "FRAMEWORK", "pattern": [{"LOWER": "react"}]},
    {"label": "FRAMEWORK", "pattern": [{"LOWER": "spring"}, {"LOWER": "boot"}]},
    {"label": "SPECIALTY", "pattern": [{"LOWER": "data"}, {"LOWER": "science"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "kubernetes"}]},
    {"label": "TOOL", "pattern": [{"LOWER": "aws"}]},
]

# --- TẢI MODEL VÀ THÊM RULE-BASED MATCHER ---
nlp = spacy.load("en_core_web_sm")
ruler = nlp.add_pipe("entity_ruler", before="ner")
ruler.add_patterns(CUSTOM_SKILL_PATTERNS)
# --- End of setup ---

# HÀM ĐÃ SỬA: Tích hợp README và logic xử lý đã tối ưu
def extract_and_classify_skills_advanced(languages_data: dict, details_data: dict) -> dict:
    SKILL_CATEGORIES = defaultdict(list)
    
    # --- 1. LANGUAGE (Ngôn ngữ Lập trình) ---
    total_bytes = sum(languages_data.values())
    for lang, bytes_count in languages_data.items():
        percent = (bytes_count / total_bytes) * 100 if total_bytes else 0
        SKILL_CATEGORIES['LANGUAGE'].append({'name': lang, 'strength_percent': round(percent, 2)})

    # --- 2. Trích xuất từ Topics, Description VÀ README (Sử dụng EntityRuler) ---
    
    # KẾT HỢP TẤT CẢ VĂN BẢN TỪ CÁC NGUỒN KHÁC NHAU (topics, description, readme)
    all_text = " ".join(details_data.get('topics', [])) 
    all_text += " " + details_data.get('description', '')
    all_text += " " + details_data.get('readme', '') 
    
    doc = nlp(all_text)
    
    # 3. Phân loại theo nhãn của EntityRuler/NER
    for ent in doc.ents:
        category = None
        # Ánh xạ nhãn của EntityRuler/Custom Pattern sang 5 danh mục
        if ent.label_ in ['FRAMEWORK', 'WORK_OF_ART']: category = 'FRAMEWORK'
        elif ent.label_ in ['DOMAIN', 'ORG']: category = 'DOMAIN'
        elif ent.label_ in ['SPECIALTY', 'SKILL']: category = 'SPECIALTY'
        elif ent.label_ in ['TOOL', 'PRODUCT']: category = 'TOOL'
        
        if category:
            SKILL_CATEGORIES[category].append({'name': ent.text, 'source': 'NLP/Ruler'})
    
    # --- 4. Áp dụng Normalization (Loại bỏ trùng lặp) ---
    final_skills = defaultdict(list)
    seen_names = set()

    for category, skills in SKILL_CATEGORIES.items():
        for skill in skills:
            # Tạm thời, sử dụng tên hiện tại làm tên chuẩn
            normalized_name = skill['name']
            
            if normalized_name not in seen_names:
                final_skills[category].append({'name': normalized_name, 'source': skill.get('source', 'NLP')})
                seen_names.add(normalized_name)
                
    return final_skills

if __name__ == '__main__':
    # Phần này sẽ chạy trong main_pipeline.py, nhưng để test thì chạy trực tiếp
    print("WARNING: Để chạy file này, bạn cần chạy Ingest trước!")