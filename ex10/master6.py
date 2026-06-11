import socket
import json
import threading
import time

# =====================================================================
# CẤU HÌNH & BỘ ĐO ĐẠC HIỆU NĂNG
# =====================================================================
SCHEDULING_POLICY = "FIFO"  # "FIFO" | "ROUND_ROBIN" | "LEAST_LOADED"

data_lock = threading.Lock()
last_rr_index = -1
system_start_time = time.time()
metrics_printed = False # Cờ đánh dấu để tránh in bảng kết quả nhiều lần

workers_table = {} 
# Bổ sung trường start_time và end_time để bấm giờ từng Task
tasks_queue = [
    {"task_id": 10, "type": "prime_count", "input": 300000, "status": "READY", "assigned_worker": None, "start_time": None, "end_time": None},
    {"task_id": 11, "type": "matrix_mult", "input": 100, "status": "READY", "assigned_worker": None, "start_time": None, "end_time": None},
    {"task_id": 12, "type": "monte_carlo", "input": 3000000, "status": "READY", "assigned_worker": None, "start_time": None, "end_time": None},
    {"task_id": 13, "type": "prime_count", "input": 200000, "status": "READY", "assigned_worker": None, "start_time": None, "end_time": None},
    {"task_id": 14, "type": "matrix_mult", "input": 80, "status": "READY", "assigned_worker": None, "start_time": None, "end_time": None},
]

def check_and_print_metrics():
    """Hàm kiểm tra xem tất cả các tác vụ đã xong chưa để xuất báo cáo"""
    global metrics_printed
    if metrics_printed:
        return

    all_done = all(task["status"] == "COMPLETED" for task in tasks_queue)
    if all_done:
        metrics_printed = True
        total_cluster_time = time.time() - system_start_time
        
        print("\n" + "="*60)
        print(f"   BÁO CÁO HIỆU NĂNG HỆ THỐNG - THUẬT TOÁN: {SCHEDULING_POLICY}")
        print("="*60)
        print(f"{'Task ID':<10}{'Loại Tác Vụ':<15}{'Worker ID':<12}{'Thời gian xử lý (s)':<20}")
        print("-"*60)
        
        total_turnaround = 0
        for task in tasks_queue:
            duration = task["end_time"] - task["start_time"]
            total_turnaround += duration
            print(f"{task['task_id']:<10}{task['type']:<15}{task['assigned_worker']:<12}{duration:<20.4f}")
            
        avg_turnaround = total_turnaround / len(tasks_queue)
        print("-"*60)
        print(f"▶ Thời gian hoàn thành trung bình (Avg Turnaround): {avg_turnaround:.4f} giây")
        print(f"▶ Tổng thời gian chạy cụm (Total Cluster Time)   : {total_cluster_time:.4f} giây")
        print("="*60 + "\n")

# =====================================================================
# LUỒNG LẬP LỊCH & ĐIỀU PHỐI (Bấm giờ khi giao việc)
# =====================================================================
def scheduler_thread():
    print(f"[THREAD 2] Bộ lập lịch chạy chiến lược: {SCHEDULING_POLICY}")
    while True:
        time.sleep(0.1)
        with data_lock:
            next_task = None
            for task in tasks_queue:
                if task["status"] == "READY":
                    next_task = task
                    break
            if not next_task:
                check_and_print_metrics() # Kiểm tra xem đã hoàn thành hết chưa
                continue

            alive_and_free_workers = [w_id for w_id, info in workers_table.items() if info["alive"] and info["current_load"] == 0]
            if not alive_and_free_workers:
                continue

            # Áp dụng thuật toán chọn Worker từ Bước 5
            target_worker_id = None
            if SCHEDULING_POLICY == "FIFO":
                target_worker_id = alive_and_free_workers[0]
            elif SCHEDULING_POLICY == "ROUND_ROBIN":
                global last_rr_index
                all_alive = [w_id for w_id, info in workers_table.items() if info["alive"]]
                if all_alive:
                    last_rr_index = (last_rr_index + 1) % len(all_alive)
                    cand = all_alive[last_rr_index]
                    target_worker_id = cand if cand in alive_and_free_workers else alive_and_free_workers[0]
            elif SCHEDULING_POLICY == "LEAST_LOADED":
                target_worker_id = min(alive_and_free_workers, key=lambda w_id: workers_table[w_id]["current_load"])

            if target_worker_id:
                # 💡 BẤM GIỜ: Ghi nhận thời điểm bắt đầu giao việc
                next_task["start_time"] = time.time()
                next_task["status"] = "RUNNING"
                next_task["assigned_worker"] = target_worker_id
                workers_table[target_worker_id]["current_load"] = 1
                
                task_message = {"type": "TASK", "task_id": next_task["task_id"], "operation": next_task["type"], "input": next_task["input"]}
                try:
                    workers_table[target_worker_id]["socket"].sendall(json.dumps(task_message).encode('utf-8'))
                except Exception: pass

# =====================================================================
# NHẬN KẾT QUẢ (Bấm giờ khi hoàn thành)
# =====================================================================
def handle_worker_messages(conn, worker_id):
    while True:
        try:
            raw_data = conn.recv(4096)
            if not raw_data: break
            message = json.loads(raw_data.decode('utf-8'))
            
            if message.get("type") == "RESULT":
                with data_lock:
                    t_id = message.get("task_id")
                    for task in tasks_queue:
                        if task["task_id"] == t_id:
                            task["status"] = "COMPLETED"
                            # 💡 BẤM GIỜ: Ghi nhận thời điểm nhận được báo cáo thành công
                            task["end_time"] = time.time()
                    if worker_id in workers_table:
                        workers_table[worker_id]["current_load"] = 0
            elif message.get("type") == "HEARTBEAT":
                with data_lock:
                    if worker_id in workers_table: workers_table[worker_id]["last_heartbeat"] = time.time()
        except Exception: break
    conn.close()

# Các hàm phụ trợ giữ nguyên từ bước trước để đảm bảo tính gọn gàng
def accept_connections_thread():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(('127.0.0.1', 5000))
    s.listen(5)
    while True:
        c, addr = s.accept()
        try:
            raw = c.recv(1024)
            if raw:
                msg = json.loads(raw.decode('utf-8'))
                if msg.get("type") == "REGISTER":
                    w_id = msg.get("worker_id")
                    with data_lock: workers_table[w_id] = {"worker_id": w_id, "alive": True, "current_load": 0, "last_heartbeat": time.time(), "socket": c}
                    threading.Thread(target=handle_worker_messages, args=(c, w_id), daemon=True).start()
        except Exception: pass

if __name__ == "__main__":
    threading.Thread(target=accept_connections_thread, daemon=True).start()
    threading.Thread(target=scheduler_thread, daemon=True).start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt: print("\nTắt Server.")
