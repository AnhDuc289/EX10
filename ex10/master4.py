import socket
import json
import threading
import time

# =====================================================================
# CẤU TRÚC DỮ LIỆU & KHÓA ĐỒNG BỘ (MUTEX LOCK)
# =====================================================================
data_lock = threading.Lock()  # Mutex bảo vệ cấu trúc dữ liệu chung

workers_table = {}  # { worker_id: { "alive": Bool, "current_load": Int, "last_heartbeat": Float, "socket": socket } }
tasks_queue = [
    {"task_id": 10, "type": "prime_count", "input": 300000, "status": "READY", "assigned_worker": None},
    {"task_id": 11, "type": "matrix_mult", "input": 100, "status": "READY", "assigned_worker": None},
    {"task_id": 12, "type": "monte_carlo", "input": 3000000, "status": "READY", "assigned_worker": None}
]

# =====================================================================
# LUỒNG 1: CHUYÊN LẮNG NGHE & NHẬN TIN NHẮN (Accept & Receive Connections)
# =====================================================================
def handle_worker_messages(conn, worker_id):
    """Mỗi Worker sẽ có một luồng phụ này để liên tục đọc tin nhắn gửi lên"""
    while True:
        try:
            raw_data = conn.recv(2048)
            if not raw_data:
                break
            
            message = json.loads(raw_data.decode('utf-8'))
            msg_type = message.get("type")

            if msg_type == "HEARTBEAT":
                with data_lock:
                    if worker_id in workers_table and workers_table[worker_id]["alive"]:
                        workers_table[worker_id]["last_heartbeat"] = time.time()
                        # In log nhỏ gọn để tránh ngập màn hình
                        print(f"[HEARTBEAT] Worker {worker_id} vẫn ổn định.")

            elif msg_type == "RESULT":
                with data_lock:
                    t_id = message.get("task_id")
                    print(f"\n[MASTER] [OK] Nhận kết quả Task {t_id} từ Worker {worker_id}: {message.get('output')}")
                    
                    # Cập nhật trạng thái Task và giải phóng Worker
                    for task in tasks_queue:
                        if task["task_id"] == t_id:
                            task["status"] = "COMPLETED"
                    if worker_id in workers_table:
                        workers_table[worker_id]["current_load"] = 0

        except (ConnectionResetError, json.JSONDecodeError):
            break

    # Nếu thoát vòng lặp tức là kết nối bị đứt đột ngột
    conn.close()

def accept_connections_thread():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('127.0.0.1', 5000))
    server_socket.listen(5)
    print("[THREAD 1] Đang lắng nghe kết nối kết nối mạng...")

    while True:
        conn, addr = server_socket.accept()
        try:
            raw_data = conn.recv(1024)
            if raw_data:
                message = json.loads(raw_data.decode('utf-8'))
                if message.get("type") == "REGISTER":
                    w_id = message.get("worker_id")
                    
                    with data_lock:
                        workers_table[w_id] = {
                            "worker_id": w_id,
                            "alive": True,
                            "current_load": 0,
                            "last_heartbeat": time.time(),
                            "socket": conn
                        }
                    print(f"\n[MASTER] Worker {w_id} đăng ký thành công từ {addr}")
                    
                    # Tạo một luồng riêng để liên tục nghe Heartbeat/Result từ Worker này
                    t = threading.Thread(target=handle_worker_messages, args=(conn, w_id), daemon=True)
                    t.start()
        except Exception as e:
            print(f"Lỗi kết nối: {e}")

# =====================================================================
# LUỒNG 2: BỘ LẬP LỊCH CHUYÊN ĐI GIAO VIỆC (Scheduler Thread)
# =====================================================================
def scheduler_thread():
    print("[THREAD 2] Bộ lập lịch FIFO đã kích hoạt.")
    while True:
        time.sleep(1) # Cứ 1 giây quét tìm việc một lần
        
        with data_lock:
            # 1. Tìm tác vụ đang READY
            next_task = None
            for task in tasks_queue:
                if task["status"] == "READY":
                    next_task = task
                    break
            
            if not next_task:
                continue # Không có việc thì đợi lượt sau

            # 2. Tìm Worker đang rảnh (alive=True và current_load=0)
            target_worker_id = None
            for w_id, info in workers_table.items():
                if info["alive"] and info["current_load"] == 0:
                    target_worker_id = w_id
                    break
            
            # 3. Tiến hành giao việc nếu đủ điều kiện
            if target_worker_id:
                next_task["status"] = "RUNNING"
                next_task["assigned_worker"] = target_worker_id
                workers_table[target_worker_id]["current_load"] = 1
                
                task_message = {
                    "type": "TASK",
                    "task_id": next_task["task_id"],
                    "operation": next_task["type"],
                    "input": next_task["input"]
                }
                
                try:
                    worker_socket = workers_table[target_worker_id]["socket"]
                    worker_socket.sendall(json.dumps(task_message).encode('utf-8'))
                    print(f"\n[SCHEDULER] Đã giao Task {next_task['task_id']} cho Worker {target_worker_id}")
                except Exception:
                    print(f"[SCHEDULER] Lỗi gửi tác vụ tới Worker {target_worker_id}")

# =====================================================================
# LUỒNG 3: GIÁM SÁT SỐNG CÒN (Heartbeat Monitor Thread)
# =====================================================================
def heartbeat_monitor_thread():
    print("[THREAD 3] Bộ giám sát Sức Khỏe Heartbeat đã kích hoạt.")
    while True:
        time.sleep(2) # Định kỳ 2 giây đi kiểm tra một lần
        now = time.time()
        
        with data_lock:
            for w_id, info in workers_table.items():
                if info["alive"]:
                    # Mục 6: Nếu Quá 6 giây không có Heartbeat -> Coi như đã chết
                    if now - info["last_heartbeat"] > 6.0:
                        info["alive"] = False
                        print(f"\n⚡ [MONITOR] !!! CẢNH BÁO: Worker {w_id} đã mất liên lạc (Mất Heartbeat > 6s) !!!")
                        
                        # [Mục 7: Khôi phục lỗi cơ bản] Tìm việc đang làm dở của nó để ném lại hàng đợi
                        for task in tasks_queue:
                            if task["assigned_worker"] == w_id and task["status"] == "RUNNING":
                                task["status"] = "READY"
                                task["assigned_worker"] = None
                                print(f"🔄 [RECOVERY] Đã giải cứu Task {task['task_id']} quay lại hàng đợi READY.")

# =====================================================================
# KHỞI CHẠY TẤT CẢ LUỒNG CỦA MASTER
# =====================================================================
if __name__ == "__main__":
    t1 = threading.Thread(target=accept_connections_thread, daemon=True)
    t2 = threading.Thread(target=scheduler_thread, daemon=True)
    t3 = threading.Thread(target=heartbeat_monitor_thread, daemon=True)
    
    t1.start()
    t2.start()
    t3.start()
    
    # Giữ luồng chính luôn chạy
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MASTER] Đang tắt hệ thống...")
