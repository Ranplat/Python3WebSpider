import json
import requests
from requests.exceptions import RequestException
from bs4 import BeautifulSoup
import time
import re
import signal
import sys
import os

# 全局变量
exit_flag = False  # 用于优雅退出
PRINT_TO_CONSOLE = True  # 设置为True则在终端打印爬取结果，False则不打印
BASE_URL = 'https://gaoloumi.cc/forum.php?mod=viewthread&tid=3335844&extra=page%3D1'  # 基本URL地址


def signal_handler(sig, frame):
    """处理中断信号"""
    global exit_flag
    print("\n接收到中断信号，正在优雅退出...")
    exit_flag = True


# 注册信号处理器
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def get_one_page(url, retries=3):
    """获取页面内容，增加重试机制"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    for attempt in range(retries):
        try:
            # 随机延时，模拟人类行为
            time.sleep(1 + (attempt * 2))  # 递增延时

            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                response.encoding = 'gbk'
                return response.text
            elif response.status_code == 404:
                print(f"页面不存在: {url}")
                return None
            else:
                print(f"请求失败，状态码: {response.status_code}，尝试次数: {attempt + 1}")

        except RequestException as e:
            print(f"请求异常: {e}，尝试次数: {attempt + 1}")
        except Exception as e:
            print(f"其他异常: {e}，尝试次数: {attempt + 1}")

        if attempt < retries - 1:
            time.sleep(2 ** attempt)  # 指数退避

    return None


def get_total_pages(html):
    """从第一页获取总页数 - 改进的动态解析方法"""
    if not html:
        return 1

    try:
        soup = BeautifulSoup(html, 'lxml')

        # 方法1: 查找包含页数信息的label元素
        page_labels = soup.find_all('label')
        for label in page_labels:
            text = label.get_text()
            # 匹配多种形式的页数信息
            patterns = [
                r'/\s*(\d+)\s*页',  # 匹配 " / 18 页"
                r'共\s*(\d+)\s*页',  # 匹配 "共 18 页"
                r'\d+/\s*(\d+)\s*',  # 匹配 "1/18"
            ]
            for pattern in patterns:
                match = re.search(pattern, text)
                if match:
                    return int(match.group(1))

        # 方法2: 查找分页链接中的最大页码
        page_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            page_match = re.search(r'page=(\d+)', href)
            if page_match:
                page_num = int(page_match.group(1))
                max_page = max(max_page, page_num)

        if max_page > 1:
            return max_page

        # 方法3: 查找class包含'last'的链接（通常指最后一页）
        last_links = soup.find_all('a', class_=re.compile(r'last', re.I))
        for link in last_links:
            text = link.get_text()
            page_match = re.search(r'(\d+)', text)
            if page_match:
                return int(page_match.group(1))

        # 方法4: 查找所有数字，可能是页数
        text_content = soup.get_text()
        # 查找类似 "第 1 页" 到 "第 N 页" 的模式
        page_numbers = re.findall(r'第\s*(\d+)\s*页', text_content)
        if page_numbers:
            return max([int(n) for n in page_numbers])

        # 方法5: 查找所有href中的page参数
        all_links = soup.find_all('a', href=True)
        page_nums = []
        for link in all_links:
            href = link['href']
            page_match = re.search(r'page=(\d+)', href)
            if page_match:
                page_nums.append(int(page_match.group(1)))
        if page_nums:
            return max(page_nums)

    except Exception as e:
        print(f"获取总页数失败: {e}")

    # 如果所有方法都失败，返回一个合理的默认值
    # 但我们不硬编码具体数字，而是返回一个标志值
    print("警告：无法自动检测总页数，将使用动态检测方法")
    return None  # 表示需要动态检测


def dynamic_page_detection():
    """
    动态检测页面数量，通过逐个尝试页面直到找不到为止
    """
    print("正在动态检测总页数...")
    page = 1
    while True:
        # 基于基本URL构造页面URL
        url = f"{BASE_URL}&page={page}"
        print(f"检测第 {page} 页...")
        html = get_one_page(url)

        if not html:
            print(f"第 {page} 页不存在或无法访问")
            break

        # 检查页面是否包含帖子内容
        if "帖子不存在" in html or "没有找到指定的主题" in html or len(html) < 1000:
            print(f"第 {page} 页无有效内容")
            break

        page += 1

        # 防止无限循环
        if page > 1000:
            print("达到最大检测页数限制")
            break

        # 添加延时避免请求过于频繁
        time.sleep(1)

    total_pages = page - 1
    print(f"动态检测到总页数: {total_pages}")
    return total_pages


def clean_content(content):
    """清理回复内容，过滤掉引用的其他用户发言信息和编辑信息"""
    if not content:
        return content

    # 删除类似"用户名 发表于 时间"的引用格式
    content = re.sub(r'[^\s\n]+?\s*发表于\s*\d{4}-\d{1,2}-\d{1,2}\s*\d{1,2}:\d{1,2}(:\d{1,2})?', '', content)

    # 删除可能残留的引用格式
    content = re.sub(r'.*?发表于\s*.*?[\r\n]+', '', content)

    # 删除编辑信息，如"本帖最后由 ... 于 ... 编辑"
    content = re.sub(r'本帖最后由\s+.*?\s+于\s+.*?编辑', '', content)

    # 删除类似"xxx 于 2025-9-17 13:21"的编辑信息
    content = re.sub(r'.*?于\s+\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}.*?编辑', '', content)

    # 删除"最后编辑"相关的内容
    content = re.sub(r'最后编辑.*', '', content)

    # 删除"本帖最后由 xxx 于 ..."格式的编辑信息
    content = re.sub(r'本帖最后由\s+.*?\s+于\s+\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}', '', content)

    # 删除单独的编辑时间信息
    content = re.sub(r'于\s+\d{4}-\d{1,2}-\d{1,2}\s+\d{1,2}:\d{1,2}', '', content)

    # 清理多余的空白行
    content = re.sub(r'\n\s*\n', '\n', content)
    content = content.strip()

    return content


def parse_one_page(html, page_num):
    """解析页面内容，添加页码参数用于计算实际楼层"""
    if html is None:
        return []

    try:
        soup = BeautifulSoup(html, 'lxml')

        # 存储解析结果
        results = []

        # 查找所有回复元素
        post_elements = soup.find_all('div', id=re.compile(r'post_\d+'))

        # 每页默认显示15个回复
        posts_per_page = 15

        # 遍历所有回复元素并处理内容
        for index, post_element in enumerate(post_elements):
            try:
                # 计算实际楼层：(页码-1)*每页回复数 + 当前页面中的序号
                floor = (page_num - 1) * posts_per_page + index + 1

                # 提取发帖人
                author_element = post_element.find('a', class_='xw1')
                author = '未知'
                if author_element:
                    author = author_element.get_text()

                # 提取发帖时间
                post_time_element = post_element.find('em', id=re.compile(r'authorposton\d+'))
                post_time = '未知时间'
                if post_time_element:
                    time_text = post_time_element.get_text()
                    post_time = re.sub(r'发表于\s*', '', time_text)

                # 提取回复内容
                content_element = post_element.find('td', class_='t_f')
                content = '无内容'
                if content_element:
                    content = content_element.get_text(strip=True)
                    # 清理内容，过滤掉引用信息和编辑信息
                    content = clean_content(content)

                # 构造结果字典
                result_item = {
                    'floor': floor,
                    'author': author,
                    'post_time': post_time,
                    'content': content
                }
                results.append(result_item)

            except Exception as e:
                print(f"处理单个回复时出错: {e}")
                continue

        return results  # 返回解析结果列表

    except Exception as e:
        print(f"解析页面时出错: {e}")
        return []  # 出错时返回空列表


def write_to_file(content):
    """写入文件"""
    with open('result.txt', 'a', encoding='utf-8') as f:
        f.write(json.dumps(content, ensure_ascii=False) + '\n')


def print_post_info(post_item):
    """打印帖子信息到终端"""
    if PRINT_TO_CONSOLE:
        print("=" * 60)
        print(f"楼层号: {post_item['floor']}")
        print(f"发帖人: {post_item['author']}")
        print(f"发帖时间: {post_item['post_time']}")
        print(f"内容: {post_item['content']}")
        print("=" * 60)


def write_progress(current_page, total_pages):
    """写入进度信息"""
    with open('progress.txt', 'w', encoding='utf-8') as f:
        f.write(f"{current_page}/{total_pages}")


def read_progress():
    """读取进度信息"""
    if os.path.exists('progress.txt'):
        try:
            with open('progress.txt', 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if '/' in content:
                    current, total = content.split('/')
                    return int(current), int(total)
        except Exception as e:
            print(f"读取进度文件失败: {e}")
    return 1, 1


def main(offset, total_pages=None):
    """主函数"""
    global exit_flag

    # 基于基本URL构造页面URL
    url = f"{BASE_URL}&page={offset}"
    if total_pages:
        print(f"正在处理第 {offset}/{total_pages} 页: {url}")
    else:
        print(f"正在处理第 {offset} 页: {url}")

    html = get_one_page(url)

    if html is None:
        print(f"获取第 {offset} 页内容失败")
        return False

    # 获取解析结果，传递页码用于计算实际楼层
    items = parse_one_page(html, offset)

    # 处理每个解析项
    for item in items:
        # 打印到终端（如果开关打开）
        print_post_info(item)
        # 写入文件
        write_to_file(item)

    print(f"第 {offset} 页处理完成，共 {len(items)} 条回复")
    return True


def crawl_all_pages():
    """爬取所有页面"""
    global exit_flag

    # 获取第一页以确定总页数
    first_url = f"{BASE_URL}&page=1"
    print("正在获取总页数...")
    first_page_html = get_one_page(first_url)

    if not first_page_html:
        print("无法获取第一页内容，退出程序")
        return

    total_pages = get_total_pages(first_page_html)

    # 如果无法解析总页数，使用动态检测
    if total_pages is None:
        total_pages = dynamic_page_detection()
    else:
        print(f"检测到总页数: {total_pages}")

    # 读取上次进度
    start_page, _ = read_progress()
    if start_page > 1:
        resume = input(f"检测到上次爬取到第 {start_page - 1} 页，是否从第 {start_page} 页继续? (y/n): ")
        if resume.lower() != 'y':
            start_page = 1

    print(f"开始从第 {start_page} 页爬取...")

    # 爬取所有页面
    success_count = 0
    for i in range(start_page, total_pages + 1):
        # 检查是否需要退出
        if exit_flag:
            print("程序被中断，保存当前进度...")
            write_progress(i, total_pages)
            break

        # 处理当前页面
        if main(i, total_pages):
            success_count += 1
        else:
            print(f"第 {i} 页处理失败")

        # 保存进度
        write_progress(i, total_pages)

        # 随机延时，避免被反爬虫机制检测
        if i < total_pages:  # 最后一页不需要延时
            delay = 1 + (hash(str(i)) % 3)  # 1-3秒随机延时
            print(f"等待 {delay} 秒后继续...")
            time.sleep(delay)

    print(f"爬取完成！成功处理 {success_count}/{total_pages - start_page + 1} 页")
    if not exit_flag:
        # 爬取完成，删除进度文件
        if os.path.exists('progress.txt'):
            os.remove('progress.txt')


if __name__ == '__main__':
    crawl_all_pages()
