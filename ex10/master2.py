import socket
import json

# =====================================================================
# 1. CẤU TRÚC DỮ LIỆU NỘI BỘ CỦA MASTER (Theo Mục 8 của đề bài)
# =====================================================================

# WorkerTable: Lưu trữ thông tin các Worker dưới dạng Dictionary
# Cấu trúc mỗi WorkerInfo: { worker_id: { "alive": Bool, "current_load": Int, "last_heartbeat": Float } }
workers_table = {}

# Task Queue: Danh sách các tác vụ cần xử lý. Đi từ trên xuống dưới (FIFO)
# Trạng thái (status) gồm: READY (Sẵn sàng) -> RUNNING (Đang chạy) -> COMPLETED (Hoàn thành)
tasks_queue = [
    {"task_id": 10, "type": "factorial", "input": 1000, "status": "READY", "assigned_worker": None},
    {"task_id": 11, "type": "matrix_mult", "input": "200x200", "status": "READY", "assigned_worker": None},
    {"task_id": 12, "type": "monte_carlo", "input": 10000000, "status": "READY", "assigned_worker": None}
]

def get_next_task_fifo():
    """Thuật toán lập lịch FIFO: Tìm tác vụ đầu tiên có trạng thái READY"""
    for task in tasks_queue:
        if task["status"] == "READY":
            return task
    return None

# =====================================================================
# 2. KHỞI CHẠY MASTER SERVER
# =====================================================================
def start_master():
    master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    HOST = '127.0.0.1'
    PORT = 5000
    
    # Cho phép tái sử dụng cổng nhanh chóng khi khởi động lại server
    master_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    master_socket.bind((HOST, PORT))
    master_socket.listen(5)
    print(f"[MASTER] Bộ lập lịch FIFO đang lắng nghe tại {HOST}:{PORT}...")

    try:
        while True:
            conn, addr = master_socket.accept()
            raw_data = conn.recv(1024)
            if not raw_data:
                conn.close()
                continue

            message = json.loads(raw_data.decode('utf-8'))

            # Xử lý khi nhận được gói tin REGISTER từ Worker
            if message.get("type") == "REGISTER":
                w_id = message.get("worker_id")
                
                # Cập nhật WorkerTable (Ghi nhận trạng thái WorkerInfo)
                workers_table[w_id] = {
                    "worker_id": w_id,
                    "alive": True,
                    "current_load": 0,
                    "last_heartbeat": None
                }
                print(f"\n[MASTER] Worker {w_id} kết nối từ {addr} và đăng ký thành công.")

                # Thực hiện Lập lịch FIFO để chọn Task giao cho Worker này
                next_task = get_next_task_fifo()
                
                if next_task:
                    # Cập nhật cấu trúc dữ liệu của Task và WorkerInfo tương ứng
                    next_task["status"] = "RUNNING"
                    next_task["assigned_worker"] = w_id
                    workers_table[w_id]["current_load"] = 1

                    # Đóng gói tin nhắn gửi TASK đi (Theo Mục 4 - Protocol)
                    task_message = {
                        "type": "TASK",
                        "task_id": next_task["task_id"],
                        "operation": next_task["type"],
                        "input": next_task["input"]
                    }
                    conn.sendall(json.dumps(task_message).encode('utf-8'))
                    print(f"[MASTER] Giao Task {next_task['task_id']} ({next_task['type']}) cho Worker {w_id}")
                    
                    # Ngồi đợi Worker làm xong và trả về gói tin RESULT
                    raw_result = conn.recv(1024)
                    if raw_result:
                        result_message = json.loads(raw_result.decode('utf-8'))
                        if result_message.get("type") == "RESULT":
                            print(f"[MASTER] Nhận kết quả từ Worker {w_id} cho Task {result_message['task_id']}")
                            
                            # Cập nhật trạng thái Task thành COMPLETED
                            next_task["status"] = "COMPLETED"
                            workers_table[w_id]["current_load"] = 0
                else:
                    # Trường hợp hết việc trong hàng đợi
                    no_task_msg = {"type": "NO_TASK"}
                    conn.sendall(json.dumps(no_task_msg).encode('utf-8'))
                    print(f"[MASTER] Hàng đợi rỗng. Không có việc giao cho Worker {w_id}.")

            conn.close()
            
    except KeyboardInterrupt:
        print("\n[MASTER] Đang tắt hệ thống...")
    finally:
        master_socket.close()

if __name__ == "__main__":
    start_master()
