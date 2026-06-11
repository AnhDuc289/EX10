import socket
import json
import sys
import threading
import time
import random

# Giữ nguyên các hàm tính toán nặng từ bước trước
def compute_prime_count(n):
    count = 0
    if n < 2: return 0
    for num in range(2, n + 1):
        is_prime = True
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                is_prime = False
                break
        if is_prime: count += 1
    return f"Có {count} số nguyên tố <= {n}"

def compute_matrix_multiplication(size):
    A = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]
    B = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]
    C = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            for k in range(size): C[i][j] += A[i][k] * B[k][j]
    return f"Xong ma trận {size}x{size}. Tổng = {sum(sum(row) for row in C)}"

def compute_monte_carlo_pi(samples):
    inside_circle = 0
    for _ in range(samples):
        x, y = random.random(), random.random()
        if x**2 + y**2 <= 1.0: inside_circle += 1
    return f"Pi ước lượng = {4 * inside_circle / samples}"

# Khóa bảo vệ Socket dùng chung giữa luồng Tính toán và luồng Heartbeat
socket_lock = threading.Lock()
is_running = True

# =====================================================================
# LUỒNG PHỤ: TỰ ĐỘNG GỬI HEARTBEAT ĐỊNH KỲ (Mục 6)
# =====================================================================
def heartbeat_sender(worker_socket, worker_id):
    global is_running
    print(f"[WORKER {worker_id}] Luồng Heartbeat đã kích hoạt (Gửi định kỳ 2s).")
    while is_running:
        time.sleep(2)
        heartbeat_msg = {
            "type": "HEARTBEAT",
            "worker_id": worker_id
        }
        try:
            with socket_lock: # Khóa socket lại trước khi gửi để tránh đụng luồng khác
                worker_socket.sendall(json.dumps(heartbeat_msg).encode('utf-8'))
        except Exception:
            print(f"[WORKER {worker_id}] Không thể gửi Heartbeat. Kết nối tới Master bị lỗi.")
            is_running = False
            break

# =====================================================================
# LUỒNG CHÍNH: ĐĂNG KÝ & CHỜ NHẬN VIỆC TOÁN HỌC
# =====================================================================
def start_worker(worker_id):
    global is_running
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        worker_socket.connect(('127.0.0.1', 5000))
        
        # 1. Gửi gói tin REGISTER đăng ký danh tính ban đầu
        register_message = {"type": "REGISTER", "worker_id": worker_id, "cpu_cores": 4}
        worker_socket.sendall(json.dumps(register_message).encode('utf-8'))
        print(f"[WORKER {worker_id}] Đã đăng ký thành công.")

        # 2. Bật luồng phụ chuyên gửi Heartbeat đi
        t_heartbeat = threading.Thread(target=heartbeat_sender, args=(worker_socket, worker_id), daemon=True)
        t_heartbeat.start()

        # 3. Luồng chính rơi vào trạng thái chờ nhận TASK liên tục từ sếp
        while is_running:
            raw_data = worker_socket.recv(4096)
            if not raw_data:
                print(f"[WORKER {worker_id}] Mất kết nối từ phía Master.")
                break

            message = json.loads(raw_data.decode('utf-8'))
            
            if message.get("type") == "TASK":
                task_id = message.get("task_id")
                operation = message.get("operation")
                task_input = message.get("input")
                
                print(f"\n[WORKER {worker_id}] => Nhận Task {task_id}: [{operation}]. Tiến hành xử lý...")
                time.sleep(4)
                # Thực hiện tính toán nặng vắt kiệt CPU
                if operation == "prime_count": result_output = compute_prime_count(task_input)
                elif operation == "matrix_mult": result_output = compute_matrix_multiplication(task_input)
                elif operation == "monte_carlo": result_output = compute_monte_carlo_pi(task_input)
                else: result_output = "Task không hợp lệ"

                # Gửi trả báo cáo kết quả RESULT về cho Master
                result_message = {"type": "RESULT", "task_id": task_id, "output": result_output}
                
                with socket_lock: # Khóa socket lại để tránh tranh chấp với luồng Heartbeat
                    worker_socket.sendall(json.dumps(result_message).encode('utf-8'))
                print(f"[WORKER {worker_id}] <= Đã nộp báo cáo Task {task_id} lên Master.")

    except ConnectionRefusedError:
        print(f"[WORKER {worker_id}] Không kết nối được Master.")
    finally:
        is_running = False
        worker_socket.close()

if __name__ == "__main__":
    w_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    start_worker(w_id)
