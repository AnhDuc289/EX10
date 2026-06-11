import socket
import json
import threading
import time

# =====================================================================
# CẤU HÌNH HỆ THỐNG & CHỌN THUẬT TOÁN LẬP LỊCH
# =====================================================================
# Bạn có thể đổi thành một trong ba chế độ: "FIFO", "ROUND_ROBIN", "LEAST_LOADED"
SCHEDULING_POLICY = "ROUND_ROBIN" 

data_lock = threading.Lock() # Mutex bảo vệ dữ liệu tránh Race Condition
last_rr_index = -1           # Biến phục vụ riêng cho thuật toán Round Robin

workers_table = {} 
# Tạo danh sách nhiều tác vụ hơn để dễ nhìn thấy sự khác biệt khi lập lịch
tasks_queue = [
    {"task_id": 10, "type": "prime_count", "input": 300000, "status": "READY", "assigned_worker": None},
    {"task_id": 11, "type": "matrix_mult", "input": 100, "status": "READY", "assigned_worker": None},
    {"task_id": 12, "type": "monte_carlo", "input": 3000000, "status": "READY", "assigned_worker": None},
    {"task_id": 13, "type": "prime_count", "input": 200000, "status": "READY", "assigned_worker": None},
    {"task_id": 14, "type": "matrix_mult", "input": 80, "status": "READY", "assigned_worker": None},
]

# =====================================================================
# BA THUẬT TOÁN CHỌN WORKER (Section 3.3)
# =====================================================================

def choose_worker_fifo(alive_and_free_workers):
    """1. FIFO: Chọn Worker nào đăng ký với hệ thống sớm nhất"""
    # Vì Python 3.7+, Dictionary giữ nguyên thứ tự thêm vào, nên lấy phần tử đầu tiên là chuẩn FIFO
    return alive_and_free_workers[0]

def choose_worker_round_robin(alive_and_free_workers):
    """2. Round Robin: Chọn Worker xoay vòng theo chu kỳ"""
    global last_rr_index
    
    # Lấy danh sách TẤT CẢ các worker đang sống (bất kể rảnh hay bận) để giữ đúng chu kỳ xoay vòng
    all_alive_workers = [w_id for w_id, info in workers_table.items() if info["alive"]]
    if not all_alive_workers:
        return None
        
    # Tính toán lượt tiếp theo trong vòng tròn
    last_rr_index = (last_rr_index + 1) % len(all_alive_workers)
    next_worker_candidate = all_alive_workers[last_rr_index]
    
    # Kiểm tra xem ứng viên vòng tròn này hiện tại có đang RẢNH (free) không
    if next_worker_candidate in alive_and_free_workers:
        return next_worker_candidate
    
    # Nếu ứng viên vòng tròn đang bận, chọn đại người rảnh đầu tiên để tối ưu hiệu năng
    return alive_and_free_workers[0]

def choose_worker_least_loaded(alive_and_free_workers):
    """3. Least Loaded: Chọn Worker đang cày ít việc nhất (Load thấp nhất)"""
    # Tìm worker có current_load nhỏ nhất trong số những người đang rảnh
    target_worker = min(alive_and_free_workers, key=lambda w_id: workers_table[w_id]["current_load"])
    return target_worker

# =====================================================================
# THREAD 2: BỘ LẬP LỊCH PHÂN PHỐI TÁC VỤ ĐA THUẬT TOÁN
# =====================================================================
def scheduler_thread():
    print(f"[THREAD 2] Bộ lập lịch kích hoạt với chiến lược: {SCHEDULING_POLICY}")
    while True:
        time.sleep(0.5) # Quét tìm việc liên tục mỗi 0.5 giây
        
        with data_lock:
            # 1. Lấy tác vụ đầu tiên đang READY (Luôn duyệt Task theo thứ tự đến trước làm trước)
            next_task = None
            for task in tasks_queue:
                if task["status"] == "READY":
                    next_task = task
                    break
            if not next_task:
                continue

            # 2. Lọc danh sách các Worker ĐANG SỐNG và ĐANG RẢNH (load = 0)
            alive_and_free_workers = [
                w_id for w_id, info in workers_table.items() 
                if info["alive"] and info["current_load"] == 0
            ]
            if not alive_and_free_workers:
                continue # Không có nhân viên nào rảnh thì đợi lượt sau

            # 3. Áp dụng thuật toán lập lịch đã cấu hình để chọn Worker
            target_worker_id = None
            if SCHEDULING_POLICY == "FIFO":
                target_worker_id = choose_worker_fifo(alive_and_free_workers)
            elif SCHEDULING_POLICY == "ROUND_ROBIN":
                target_worker_id = choose_worker_round_robin(alive_and_free_workers)
            elif SCHEDULING_POLICY == "LEAST_LOADED":
                target_worker_id = choose_worker_least_loaded(alive_and_free_workers)

            # 4. Gửi việc đi nếu chọn được Worker hợp lệ
            if target_worker_id:
                next_task["status"] = "RUNNING"
                next_task["assigned_worker"] = target_worker_id
                workers_table[target_worker_id]["current_load"] = 1 # Đánh dấu bận
                
                task_message = {
                    "type": "TASK",
                    "task_id": next_task["task_id"],
                    "operation": next_task["type"],
                    "input": next_task["input"]
                }
                try:
                    conn = workers_table[target_worker_id]["socket"]
                    conn.sendall(json.dumps(task_message).encode('utf-8'))
                    print(f"\n[SCHEDULER - {SCHEDULING_POLICY}] => Giao Task {next_task['task_id']} cho Worker {target_worker_id}")
                except Exception:
                    print(f"[SCHEDULER] Thất bại khi gửi việc tới Worker {target_worker_id}")

# =====================================================================
# CÁC LUỒNG CÒN LẠI (GIỮ NGUYÊN TỪ BƯỚC 4)
# =====================================================================
def handle_worker_messages(conn, worker_id):
    while True:
        try:
            raw_data = conn.recv(4096)
            if not raw_data: break
            message = json.loads(raw_data.decode('utf-8'))
            msg_type = message.get("type")

            if msg_type == "HEARTBEAT":
                with data_lock:
                    if worker_id in workers_table and workers_table[worker_id]["alive"]:
                        workers_table[worker_id]["last_heartbeat"] = time.time()
            elif msg_type == "RESULT":
                with data_lock:
                    t_id = message.get("task_id")
                    print(f"[MASTER] [OK] Worker {worker_id} hoàn thành Task {t_id}!")
                    for task in tasks_queue:
                        if task["task_id"] == t_id: task["status"] = "COMPLETED"
                    if worker_id in workers_table: workers_table[worker_id]["current_load"] = 0
        except Exception: break
    conn.close()

def accept_connections_thread():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(('127.0.0.1', 5000))
    server_socket.listen(5)
    print("[THREAD 1] Lắng nghe kết nối mạng tại cổng 5000...")
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
                            "worker_id": w_id, "alive": True, "current_load": 0,
                            "last_heartbeat": time.time(), "socket": conn
                        }
                    print(f"[MASTER] Worker {w_id} gia nhập hệ thống.")
                    threading.Thread(target=handle_worker_messages, args=(conn, w_id), daemon=True).start()
        except Exception: pass

def heartbeat_monitor_thread():
    while True:
        time.sleep(2)
        now = time.time()
        with data_lock:
            for w_id, info in workers_table.items():
                if info["alive"] and (now - info["last_heartbeat"] > 6.0):
                    info["alive"] = False
                    print(f"\n⚡ [MONITOR] Worker {w_id} ĐÃ CHẾT do mất Heartbeat!")
                    for task in tasks_queue:
                        if task["assigned_worker"] == w_id and task["status"] == "RUNNING":
                            task["status"] = "READY"
                            task["assigned_worker"] = None
                            print(f"🔄 [RECOVERY] Đã cứu Task {task['task_id']} về hàng đợi.")

if __name__ == "__main__":
    threading.Thread(target=accept_connections_thread, daemon=True).start()
    threading.Thread(target=scheduler_thread, daemon=True).start()
    threading.Thread(target=heartbeat_monitor_thread, daemon=True).start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt: print("\n[MASTER] Tắt Server.")
