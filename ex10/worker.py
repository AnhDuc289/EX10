import socket
import json

def register_worker():
    # 1. Khởi tạo Socket TCP của Worker
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 2. Cấu hình địa chỉ của Master để kết nối
    MASTER_HOST = '127.0.0.1'
    MASTER_PORT = 5000
    
    try:
        # 3. Kết nối tới Master
        print(f"[WORKER] Gửi yêu cầu kết nối tới Master tại {MASTER_HOST}:{MASTER_PORT}...")
        worker_socket.connect((MASTER_HOST, MASTER_PORT))
        
        # 4. Chuẩn bị tin nhắn đăng ký theo đúng yêu cầu Protocol dạng JSON
        register_message = {
            "type": "REGISTER",
            "worker_id": 3,
            "cpu_cores": 4
        }
        
        # 5. Chuyển JSON thành String, rồi mã hóa thành Byte (utf-8) để gửi qua mạng
        json_string = json.dumps(register_message)
        worker_socket.sendall(json_string.encode('utf-8'))
        print("[WORKER] Đã gửi tin nhắn đăng ký thành công!")
        
    except ConnectionRefusedError:
        print("[WORKER] Lỗi: Không thể kết nối tới Master. Sếp đã bật server chưa vậy?")
    finally:
        # 6. Đóng socket
        worker_socket.close()

if __name__ == "__main__":
    register_worker()
