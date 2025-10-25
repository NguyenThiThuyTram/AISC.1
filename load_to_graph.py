# load_to_graph.py
print("DEBUG: File load_to_graph.py đang được đọc...")

try:
    from neo4j import GraphDatabase, basic_auth
    from ingest import fetch_data_from_kaggle
    from process import process_raw_data
    import pandas as pd
    import traceback # Thêm thư viện này để in lỗi chi tiết
    print("DEBUG: Đã import thư viện thành công.")
except Exception as e:
    print(f"LỖI IMPORT NGHIÊM TRỌNG: {e}")


# --- CẤU HÌNH NEO4J ---
# !!! QUAN TRỌNG: HÃY KIỂM TRA LẠI MẬT KHẨU CỦA BẠN !!!
NEO4J_URI = "bolt://127.0.0.1:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "123456789" # !!! THAY BẰNG MẬT KHẨU THẬT CỦA BẠN !!!

def get_neo4j_driver(uri, user, password):
    """Tạo và trả về Neo4j Driver."""
    print("DEBUG: Đang chạy hàm get_neo4j_driver...")
    try:
        driver = GraphDatabase.driver(uri, auth=basic_auth(user, password))
        driver.verify_connectivity()
        print("Kết nối Neo4j thành công!")
        return driver
    except Exception as e:
        print(f"--- LỖI KẾT NỐI NEO4J: {e} ---")
        return None

def setup_ontology(driver):
    """
    Chạy các lệnh Ràng buộc (Constraint) một lần duy nhất.
    """
    print("DEBUG: Đang chạy hàm setup_ontology...")
    try:
        with driver.session() as session:
            session.run("CREATE CONSTRAINT project_id_unique IF NOT EXISTS FOR (p:Project) REQUIRE p.projectId IS UNIQUE;")
            session.run("CREATE CONSTRAINT employee_id_unique IF NOT EXISTS FOR (e:Employee) REQUIRE e.employeeId IS UNIQUE;")
            session.run("CREATE CONSTRAINT skill_name_unique IF NOT EXISTS FOR (s:Skill) REQUIRE s.name IS UNIQUE;")
        print("Ràng buộc đã được thiết lập.")
    except Exception as e:
        print(f"--- LỖI SETUP ONTOLOGY: {e} ---")

def clear_database(driver):
    """
    Xóa tất cả Node và Quan hệ cũ để chạy demo mới.
    """
    print("DEBUG: Đang chạy hàm clear_database...")
    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("Đã xóa dữ liệu cũ.")
    except Exception as e:
        print(f"--- LỖI XÓA DATABASE: {e} ---")

def load_projects_to_neo4j(driver, df):
    """
    Nạp DataFrame đã xử lý vào Neo4j bằng câu lệnh Cypher đầy đủ.
    """
    print(f"DEBUG: Đang chạy hàm load_projects_to_neo4j với {len(df)} dòng...")
    
    full_query = """
    UNWIND $rows AS row
    MERGE (p:Project {projectId: row.projectId})
    SET
        p.projectName = row.projectName,
        p.domain = row.domain,
        p.skillFocus = row.skillFocus,
        p.complexityLevel = row.complexityLevel,
        p.learningPotential = row.learningPotential,
        p.budget = row.budget,
        p.startDate = date(row.startDate),
        p.endDate = date(row.endDate),
        p.status = row.status,
        p.priority = row.priority,
        p.riskLevel = row.riskLevel,
        p.minTeamSize = row.minTeamSize,
        p.requiredRoles = row.requiredRoles,
        p.requiredExperience = row.requiredExperience,
        p.projectScore = row.projectScore
    WITH p, row.skillFocus AS skills
    UNWIND skills AS skillName
    MERGE (s:Skill {name: skillName})
    MERGE (p)-[r:REQUIRES_SKILL]->(s)
    SET 
        r.requiredLevel = CASE 
                            WHEN p.complexityLevel >= 5 THEN 'Expert'
                            WHEN p.complexityLevel >= 3 THEN 'Medium'
                            ELSE 'Low'
                          END,
        r.skillWeighting = round(rand(), 2)
    """
    
    data_rows = df.to_dict('records')
    for row in data_rows:
        row['startDate'] = row['startDate'].isoformat()
        row['endDate'] = row['endDate'].isoformat()

    try:
        with driver.session() as session:
            session.run(full_query, rows=data_rows)
        print(f"Nạp thành công {len(data_rows)} project vào Neo4j.")
    except Exception as e:
        print(f"--- LỖI NẠP DỮ LIỆU: {e} ---")


# --- HÀM CHÍNH ĐỂ CHẠY ---
if __name__ == "__main__":
    print("--- DEBUG: BẮT ĐẦU CHẠY KHỐI __main__ ---")
    
    try:
        driver = get_neo4j_driver(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        
        if driver:
            print("--- DEBUG: Driver HỢP LỆ. Bắt đầu pipeline. ---")
            setup_ontology(driver)
            clear_database(driver)
            
            raw_df = fetch_data_from_kaggle()
            
            if raw_df is not None:
                print("--- DEBUG: Kaggle Data HỢP LỆ. Bắt đầu xử lý. ---")
                processed_df = process_raw_data(raw_df)
                load_projects_to_neo4j(driver, processed_df)
                
                print("\n--- DEMO PIPELINE HOÀN TẤT ---")
                print("Kiểm tra Neo4j Browser của bạn!")
            else:
                print("--- LỖI: raw_df LÀ None (Không tải được data Kaggle). ---")
            
            driver.close()
        else:
            print("--- LỖI: Driver LÀ None (Kết nối Neo4j thất bại). ---")
    
    except Exception as e:
        print("!!!!!!!!!! ĐÃ XẢY RA LỖI TỔNG (CRITICAL ERROR) !!!!!!!!!!")
        print(f"CHI TIẾT LỖI: {e}")
        traceback.print_exc() # In ra chi tiết lỗi đầy đủ nhất