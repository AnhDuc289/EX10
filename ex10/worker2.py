import socket
import json
import sys
import time

def start_worker(worker_id):
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    MASTER_HOST = '127.0.0.1'
    MASTER_PORT = 5000
    
    try:
        worker_socket.connect((MASTER_HOST, MASTER_PORT))
        
        # 1. Gửi gói tin REGISTER đăng ký danh tính
        register_message = {
            "type": "REGISTER",
            "worker_id": worker_id,
            "cpu_cores": 4
        }
        worker_socket.sendall(json.dumps(register_message).encode('utf-8'))
        print(f"[WORKER {worker_id}] Đã gửi yêu cầu đăng ký.")

        # 2. Đợi Master phản hồi (Giao việc hoặc báo Hết việc)
        raw_data = worker_socket.recv(1024)
        if not raw_data:
            return

        message = json.loads(raw_data.decode('utf-8'))
        
        if message.get("type") == "TASK":
            task_id = message.get("task_id")
            operation = message.get("operation")
            print(f"[WORKER {worker_id}] Đã nhận được Task {task_id}: Thực hiện phép tính [{operation}]")
            
            # Giả lập thời gian làm việc cày cuốc mất 1 giây
            print(f"[WORKER {worker_id}] Đang tính toán dữ liệu...")
            time.sleep(1) 
            
            # 3. Đóng gói kết quả gửi RESULT lại cho Master (Theo Mục 4 - Protocol)
            result_message = {
                "type": "RESULT",
                "task_id": task_id,
                "output": f"SUCCESS_VALUE_OF_{operation.upper()}"
            }
            worker_socket.sendall(json.dumps(result_message).encode('utf-8'))
            print(f"[WORKER {worker_id}] Đã nộp báo cáo kết quả về Master.")
            
        elif message.get("type") == "NO_TASK":
            print(f"[WORKER {worker_id}] Sếp báo hết việc rồi, nghỉ ngơi thôi!")

    except ConnectionRefusedError:
        print(f"[WORKER {worker_id}] Không thể kết nối tới Master.")
    finally:
        worker_socket.close()

if __name__ == "__main__":
    # Đọc ID từ tham số terminal, nếu không truyền mặc định là ID = 1
    # Cách dùng: python3 worker.py 2 (để tạo Worker có ID là 2)
    w_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    start_worker(w_id)
