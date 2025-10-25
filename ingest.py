# ingest.py
import kaggle
import pandas as pd
import os
import traceback

# Dataset này chứa "Job Title" và "Job Description"
DATASET_NAME = 'andrewmvd/data-analyst-jobs'
DOWNLOAD_DIR = 'raw_data'
CSV_FILE = os.path.join(DOWNLOAD_DIR, 'DataAnalyst.csv')

def fetch_data_from_kaggle():
    """
    Tải và đọc dữ liệu thô từ Kaggle.
    Chỉ lấy 100 dòng đầu tiên để làm demo.
    """
    print(f"DEBUG: Đang chạy hàm fetch_data_from_kaggle...")
    print(f"DEBUG: Sẽ tải về {DATASET_NAME}...")
    
    # Tạo thư mục nếu chưa có
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    
    try:
        # 1. Tải dataset về thư mục raw_data
        kaggle.api.dataset_download_files(DATASET_NAME, 
                                           path=DOWNLOAD_DIR, 
                                           unzip=True)
        print("DEBUG: Kaggle API tải file thành công.")
    except Exception as e:
        print(f"--- LỖI KHI TẢI TỪ KAGGLE: {e} ---")
        print("--- HÃY ĐẢM BẢO BẠN ĐÃ ĐẶT file 'kaggle.json' vào 'C:\\Users\\Tien Le\\.kaggle\\' ---")
        traceback.print_exc()
        return None

    # 2. Đọc file CSV vào Pandas DataFrame
    if os.path.exists(CSV_FILE):
        try:
            df = pd.read_csv(CSV_FILE)
            df_demo = df.head(100).copy() 
            print(f"Tải và đọc thành công {len(df_demo)} dòng dữ liệu demo.")
            return df_demo
        except Exception as e:
            print(f"--- LỖI KHI ĐỌC FILE CSV: {e} ---")
            traceback.print_exc()
            return None
    else:
        print(f"--- LỖI: Không tìm thấy file {CSV_FILE} sau khi giải nén. ---")
        return None

if __name__ == "__main__":
    # Chạy file này riêng để kiểm tra tải dữ liệu
    data = fetch_data_from_kaggle()
    if data is not None:
        print("\n(TEST) Tải dữ liệu thành công. 5 dòng đầu tiên:")
        print(data.head())