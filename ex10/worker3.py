import socket
import json
import sys
import random

# =====================================================================
# LẬP TRÌNH 4 TÁC VỤ NẶNG CPU (Section 5)
# =====================================================================

def compute_prime_count(n):
    """Task A: Đếm số lượng số nguyên tố <= N"""
    count = 0
    if n < 2: return 0
    # Dùng thuật toán kiểm tra cơ bản để tốn CPU một chút
    for num in range(2, n + 1):
        is_prime = True
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                is_prime = False
                break
        if is_prime:
            count += 1
    return f"Có {count} số nguyên tố nhỏ hơn hoặc bằng {n}."

def compute_matrix_multiplication(size):
    """Task B: Nhân 2 ma trận vuông kích thước size x size"""
    # Khởi tạo ma trận ngẫu nhiên bằng list của python
    A = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]
    B = [[random.randint(1, 10) for _ in range(size)] for _ in range(size)]
    # Ma trận kết quả
    C = [[0 for _ in range(size)] for _ in range(size)]
    
    # 3 vòng lặp kinh điển tính nhân ma trận tiêu tốn CPU
    for i in range(size):
        for j in range(size):
            for k in range(size):
                C[i][j] += A[i][k] * B[k][j]
                
    # Để tránh gửi chuỗi quá dài qua Socket, ta trả về tổng các phần tử của ma trận kết quả
    total_sum = sum(sum(row) for row in C)
    return f"Đã nhân xong ma trận {size}x{size}. Tổng giá trị ma trận kết quả = {total_sum}"

def compute_monte_carlo_pi(samples):
    """Task C: Ước lượng số Pi bằng phương pháp Monte Carlo với số lượng mẫu lớn"""
    inside_circle = 0
    for _ in range(samples):
        x = random.random()
        y = random.random()
        if x**2 + y**2 <= 1.0:
            inside_circle += 1
    pi_estimate = 4 * inside_circle / samples
    return f"Ước lượng Số Pi = {pi_estimate} (Tính toán dựa trên {samples} mẫu)"

def compute_word_count(repeat_factor):
    """Task D: Giả lập đọc một file text lớn bằng cách nhân bản văn bản và đếm tần suất từ"""
    base_text = "apple banana apple cherry banana apple mango orange cherry apple "
    # Nhân bản chuỗi lên để tạo dữ liệu khổng lồ trong bộ nhớ
    large_text = base_text * repeat_factor
    
    words = large_text.split()
    frequency = {}
    for word in words:
        frequency[word] = frequency.get(word, 0) + 1
        
    return f"Đã đếm xong {len(words)} từ. Kết quả một số từ tiêu biểu: {dict(list(frequency.items())[:3])}"

# =====================================================================
# ĐIỀU PHỐI VÀ CHẠY WORKER
# =====================================================================

def start_worker(worker_id):
    worker_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    MASTER_HOST = '127.0.0.1'
    MASTER_PORT = 5000
    
    try:
        worker_socket.connect((MASTER_HOST, MASTER_PORT))
        
        register_message = {"type": "REGISTER", "worker_id": worker_id, "cpu_cores": 4}
        worker_socket.sendall(json.dumps(register_message).encode('utf-8'))
        print(f"[WORKER {worker_id}] Đã gửi yêu cầu đăng ký.")

        raw_data = worker_socket.recv(1024)
        if not raw_data: return

        message = json.loads(raw_data.decode('utf-8'))
        
        if message.get("type") == "TASK":
            task_id = message.get("task_id")
            operation = message.get("operation")
            task_input = message.get("input")
            
            print(f"[WORKER {worker_id}] Bắt đầu cày cuốc Task {task_id}: [{operation}]...")
            
            # Chọn đúng hàm toán học để chạy dựa vào yêu cầu của Master
            if operation == "prime_count":
                result_output = compute_prime_count(task_input)
            elif operation == "matrix_mult":
                result_output = compute_matrix_multiplication(task_input)
            elif operation == "monte_carlo":
                result_output = compute_monte_carlo_pi(task_input)
            elif operation == "word_count":
                result_output = compute_word_count(task_input)
            else:
                result_output = "Tác vụ không hợp lệ."

            # Gửi kết quả thực tế về cho Master
            result_message = {
                "type": "RESULT",
                "task_id": task_id,
                "output": result_output
            }
            worker_socket.sendall(json.dumps(result_message).encode('utf-8'))
            print(f"[WORKER {worker_id}] Đã xử lý xong và nộp kết quả lên Master.")
            
        elif message.get("type") == "NO_TASK":
            print(f"[WORKER {worker_id}] Hết việc, tắt máy đi chơi thôi!")

    except ConnectionRefusedError:
        print(f"[WORKER {worker_id}] Thất bại: Không kết nối được Master.")
    finally:
        worker_socket.close()

if __name__ == "__main__":
    w_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    start_worker(w_id)
