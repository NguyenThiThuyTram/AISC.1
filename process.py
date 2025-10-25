# process.py
import pandas as pd
import random
from datetime import datetime, timedelta
from skills_taxonomy import SKILL_TAXONOMY

# --- Helper 1: Trích xuất Kỹ năng (Logic NLP) ---
def extract_skills_from_text(text):
    if not isinstance(text, str):
        return []
    found_skills = set()
    text_lower = text.lower()
    for normalized_skill, keywords in SKILL_TAXONOMY.items():
        for keyword in keywords:
            if keyword in text_lower:
                found_skills.add(normalized_skill)
    return list(found_skills)

# --- Helper 2: Suy luận từ Chức danh (IT-Specific) ---
def analyze_it_job_title(title_str):
    title_lower = str(title_str).lower()
    complexity = 3 # Mặc định
    experience = 3 # Mặc định
    roles = [title_str] # Mặc định
    
    if 'senior' in title_lower or 'lead' in title_lower or 'sr.' in title_lower:
        complexity = 5
        experience = 5
        roles.append('Senior Level')
    elif 'junior' in title_lower or 'entry' in title_lower or 'jr.' in title_lower:
        complexity = 2
        experience = 1
        roles.append('Junior Level')
    elif 'manager' in title_lower or 'head of' in title_lower:
        complexity = 4
        experience = 7
        roles.append('Manager')
        
    return complexity, experience, list(set(roles))

# --- Helper 3: Giả lập Dòng thời gian & Trạng thái ---
def mock_project_timeline():
    status = random.choice(['Planning', 'In Progress', 'Completed'])
    if status == 'Planning':
        start_date = datetime.now() + timedelta(days=random.randint(10, 60))
        end_date = start_date + timedelta(days=random.randint(90, 365))
        project_score = 0.0 # Chưa hoàn thành
    elif status == 'In Progress':
        start_date = datetime.now() - timedelta(days=random.randint(30, 90))
        end_date = datetime.now() + timedelta(days=random.randint(90, 365))
        project_score = 0.0 # Chưa hoàn thành
    else: # Completed
        end_date = datetime.now() - timedelta(days=random.randint(10, 90))
        start_date = end_date - timedelta(days=random.randint(90, 365))
        project_score = round(random.uniform(0.7, 0.95), 2) # Điểm số khi đã hoàn thành
        
    # Trả về đối tượng date, không phải datetime
    return status, start_date.date(), end_date.date(), project_score

# --- Helper 4: Giả lập Thuộc tính Chiến lược (IT-Specific) ---
def mock_strategic_attributes(complexity, skills_count):
    # Rủi ro cao nếu độ phức tạp cao và cần nhiều kỹ năng
    risk = 'Low'
    if complexity >= 4 and skills_count >= 5:
        risk = 'High'
    elif complexity >= 3:
        risk = 'Medium'
        
    # Tiềm năng học hỏi cao nếu độ phức tạp cao
    learning_potential = round(complexity / 5.0, 2)
    
    return risk, learning_potential

# --- Hàm Chính: Xử lý Dữ liệu ---
def process_raw_data(df):
    """
    Nhận DataFrame thô, trả về DataFrame đã xử lý
    với đầy đủ các thuộc tính Project.
    """
    print("Bắt đầu xử lý và chuẩn hóa dữ liệu...")
    
    processed_data = []
    
    # Đổi tên cột để dễ đọc
    df = df.rename(columns={'Job Title': 'projectName_raw', 
                            'Company Name': 'domain_raw', 
                            'Job Description': 'description_raw'})
    
    for index, row in df.iterrows():
        # 1. Trích xuất kỹ năng (Cốt lõi)
        skills = extract_skills_from_text(row['description_raw'])
        
        # 2. Suy luận từ Chức danh
        complexity, req_exp, req_roles = analyze_it_job_title(row['projectName_raw'])
        
        # 3. Giả lập Dòng thời gian
        status, start, end, score = mock_project_timeline()
        
        # 4. Giả lập Thuộc tính Chiến lược
        risk, learning = mock_strategic_attributes(complexity, len(skills))

        # 5. Tạo đối tượng Project hoàn chỉnh (theo đúng Ontology)
        project_record = {
            'projectId': f'Kaggle-P{index:04d}',
            'projectName': row['projectName_raw'],
            'domain': row['domain_raw'],
            'skillFocus': skills,
            'complexityLevel': complexity,
            'learningPotential': learning,
            'budget': float(random.randint(10000, 150000)),
            'startDate': start,
            'endDate': end,
            'status': status,
            'priority': random.randint(1, 5),
            'riskLevel': risk,
            'minTeamSize': random.randint(2, 5),
            'requiredRoles': req_roles,
            'requiredExperience': req_exp,
            'projectScore': score
        }
        processed_data.append(project_record)

    print("Xử lý hoàn tất. Dữ liệu đã được làm giàu đầy đủ.")
    
    # Trả về một DataFrame mới từ dữ liệu đã xử lý
    return pd.DataFrame(processed_data)