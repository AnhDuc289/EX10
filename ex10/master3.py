import socket
import json

# Workers Table giữ nguyên
workers_table = {}

# CẬP NHẬT: Định nghĩa 4 Task thực tế theo đúng Mục 5 của đề bài
tasks_queue = [
    {"task_id": 10, "type": "prime_count", "input": 500000, "status": "READY", "assigned_worker": None}, # Giảm xuống 500k cho nhanh một chút trên python thuần
    {"task_id": 11, "type": "matrix_mult", "input": 150, "status": "READY", "assigned_worker": None},    # Ma trận 150x150
    {"task_id": 12, "type": "monte_carlo", "input": 5000000, "status": "READY", "assigned_worker": None}, # 5 triệu mẫu thử Pi
    {"task_id": 13, "type": "word_count", "input": 20000, "status": "READY", "assigned_worker": None}     # Nhân bản văn bản thành 20k dòng để đếm
]

def get_next_task_fifo():
    for task in tasks_queue:
        if task["status"] == "READY":
            return task
    return None

def start_master():
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    HOST = '127.0.0.1'
    PORT = 5000
    master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    master_socket.bind((HOST, PORT))
    master_socket.listen(5)
    print(f"[MASTER] Bộ lập lịch đang lắng nghe tại {HOST}:{PORT}...")

    try:
        while True:
            conn, addr = master_socket.accept()
            raw_data = conn.recv(1024)
            if not raw_data:
                conn.close()
                continue

            message = json.loads(raw_data.decode('utf-8'))

            if message.get("type") == "REGISTER":
                w_id = message.get("worker_id")
                workers_table[w_id] = {"worker_id": w_id, "alive": True, "current_load": 0}
                print(f"\n[MASTER] Worker {w_id} đăng ký.")

                next_task = get_next_task_fifo()
                
                if next_task:
                    next_task["status"] = "RUNNING"
                    next_task["assigned_worker"] = w_id
                    
                    task_message = {
                        "type": "TASK",
                        "task_id": next_task["task_id"],
                        "operation": next_task["type"],
                        "input": next_task["input"]
                    }
                    conn.sendall(json.dumps(task_message).encode('utf-8'))
                    print(f"[MASTER] Đã giao Task {next_task['task_id']} ({next_task['type']}) cho Worker {w_id}")
                    
                    # Chờ nhận kết quả lớn từ Worker
                    # Tăng bộ đệm nhận lên 4096 hoặc nhiều hơn phòng khi chuỗi kết quả dài
                    raw_result = conn.recv(4096)
                    if raw_result:
                        result_message = json.loads(raw_result.decode('utf-8'))
                        if result_message.get("type") == "RESULT":
                            print(f"[MASTER] [OK] Nhận kết quả từ Worker {w_id} cho Task {result_message['task_id']}")
                            print(f"         Đáp án: {result_message['output']}")
                            next_task["status"] = "COMPLETED"
                else:
                    conn.sendall(json.dumps({"type": "NO_TASK"}).encode('utf-8'))
                    print(f"[MASTER] Hàng đợi rỗng. Không có việc cho Worker {w_id}.")

            conn.close()
            
    except KeyboardInterrupt:
        print("\n[MASTER] Đang tắt hệ thống...")
    finally:
        master_socket.close()

if __name__ == "__main__":
    start_master()
