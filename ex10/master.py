import socket
import json

def start_master():
    # 1. Khởi tạo Socket TCP
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # 2. Ràng buộc Socket với IP và Port (Ở đây dùng localhost và cổng 5000)
    HOST = '127.0.0.1'
    PORT = 5000
    master_socket.bind((HOST, PORT))
    
    # 3. Chuyển sang trạng thái lắng nghe kết nối (tối đa 5 kết nối đợi trong hàng đợi)
    master_socket.listen(5)
    print(f"[MASTER] Đang lắng nghe tại {HOST}:{PORT}...")
    
    try:
        while True:
            # 4. Chấp nhận kết nối từ một Worker
            worker_conn, worker_addr = master_socket.accept()
            print(f"[MASTER] Kết nối mới từ Worker tại địa chỉ: {worker_addr}")
            
            # 5. Nhận dữ liệu (tối đa 1024 bytes) từ Worker gửi lên
            raw_data = worker_conn.recv(1024)
            if not raw_data:
                continue
                
            # 6. Giải mã dữ liệu từ Byte -> String -> JSON (Dict)
            data_string = raw_data.decode('utf-8')
            message = json.loads(data_string)
            
            # 7. Kiểm tra nếu là tin nhắn REGISTER thì xử lý
            if message.get("type") == "REGISTER":
                worker_id = message.get("worker_id")
                cpu_cores = message.get("cpu_cores")
                print(f"[MASTER] Đăng ký thành công! Worker ID: {worker_id} | Số Cores: {cpu_cores}")
                
            # Đóng kết nối với Worker này (ở Bước 1 làm đơn giản thế này đã)
            worker_conn.close()
            
    except KeyboardInterrupt:
        print("\n[MASTER] Đang đóng Server...")
    finally:
        master_socket.close()

if __name__ == "__main__":
    start_master()
