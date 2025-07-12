import subprocess


def run_cmd(cmd, shell=True, timeout=None):
    try:
        print(f"执行命令: {cmd}")
        process = subprocess.Popen(
            cmd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        stdout, _ = process.communicate(timeout=timeout)
        return stdout.strip()

    except subprocess.TimeoutExpired:
        process.kill()
        return f"命令执行超时（{timeout}秒）"
    except Exception as e:
        return f"执行命令时发生错误：{str(e)}"


def run_cmd_stream(cmd, line_callback=None, shell=True, timeout=None, max_lines=10000):
    """
    流式执行命令，支持自定义回调函数处理每一行输出

    Args:
        cmd: 要执行的命令
        line_callback: 处理每一行输出的回调函数 function(line_content)
        shell: 是否使用shell执行
        timeout: 超时时间（秒）
        max_lines: 最大处理行数

    Yields:
        str: 每一行输出
    """
    process = None
    try:
        print(f"流式执行命令: {cmd}")
        process = subprocess.Popen(
            cmd,
            shell=shell,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
        )

        line_count = 0
        for line in iter(process.stdout.readline, ""):
            line_count += 1
            if line_count > max_lines:
                print(f"达到最大行数限制 ({max_lines})，终止子进程...")
                break

            line = line.strip()
            if not line:
                continue

            # 调用回调函数处理每一行
            if line_callback:
                try:
                    line = line_callback(line)
                except Exception as e:
                    print(f"处理行失败: {line_count}, 错误: {e}")
                    continue

            yield line

    except subprocess.TimeoutExpired:
        print(f"命令执行超时（{timeout}秒）")

    except Exception as e:
        print(f"执行命令时发生错误：{str(e)}")

    finally:
        # 确保进程被正确终止
        if process and process.poll() is None:
            print("正在终止子进程...")
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print("强制杀死子进程...")
                process.kill()
                process.wait()
            print("子进程已终止")
