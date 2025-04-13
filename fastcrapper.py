import csv
import time
import requests
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class MahjongScraper:
    def __init__(self, mode, start_page, end_page, date_str):
        # 初始化爬虫，设定模式、起始页、结束页及日期
        self.mode = str(mode)  # 将模式转换为字符串
        self.start_page = start_page
        self.end_page = end_page
        self.date_str = date_str
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.fields = ['name', 'detail_url']  # CSV文件的字段
        self.seen_items = set()  # 用于跟踪已处理的条目
        self.seen_players = set()  # 用于跟踪已处理的玩家
        self.items_buffer = []  # 数据缓冲区
        self.buffer_size = 10   # 缓冲区大小
        self.processed_pages = set()  # 用于跟踪已处理的页面
        
        # 添加模式名称映射
        self.mode_names = {
            "16": "王座",
            "12": "玉",
            "9": "金",
            "15": "王朝",
            "11": "玉东",
            "8": "金东"
        }
        
        # 生成文件名：页数范围+模式名+日期
        mode_name = self.mode_names.get(self.mode, "未知")
        self.csv_path = f'data_{start_page}-{end_page}_{mode_name}_{date_str}.csv'
        
        # 生成时间戳
        self.timestamp_1, self.timestamp_2 = self._generate_timestamps()
        
        print(f"CSV文件将保存在: {self.csv_path}")  # 打印CSV文件保存位置

    def _generate_timestamps(self):
        # 生成时间戳
        date_obj = datetime.strptime(self.date_str, '%Y-%m-%d')  # 将日期字符串转换为datetime对象
        timestamp2 = int(date_obj.timestamp() * 1000)  # 转换为毫秒时间戳
        timestamp_1 = str(timestamp2)[:-6] + '999999'  # 将最后六位数替换为999999
        return timestamp_1, timestamp2  # 返回两个时间戳

    def fetch_game_data(self, page):
        try:
            params = {
                'limit': '100',
                'descending': 'true',
                'mode': self.mode,
            }
            response = requests.get(
                f'https://5-data.amae-koromo.com/api/v2/pl{page}/games/{self.timestamp_1}/{self.timestamp_2}',
                params=params,
                headers=self.headers,
                timeout=2  # 添加2秒超时
            ).json()
            return response
        except (requests.Timeout, requests.RequestException) as e:
            print(f"获取游戏数据超时或出错 (页面 {page}): {str(e)}")
            return []

    def fetch_player_records(self, accountId, gradingScore):
        try:
            params = {
                'limit': '181',
                'mode': self.mode,
                'descending': 'true',
                'tag': gradingScore,
            }
            response = requests.get(
                f'https://5-data.amae-koromo.com/api/v2/pl4/player_records/{accountId}/{self.timestamp_1}/{self.timestamp_2}',
                params=params,
                headers=self.headers,
                timeout=2  # 添加2秒超时
            ).json()
            return response
        except (requests.Timeout, requests.RequestException) as e:
            print(f"获取玩家记录超时或出错 (ID {accountId}): {str(e)}")
            return []

    def process_page(self, page):
        # 检查页面是否已经处理过
        if page in self.processed_pages:
            return
        
        # 标记页面为已处理
        self.processed_pages.add(page)
        print(f"开始处理第 {page} 页...")
        
        # 处理指定页的数据
        game_data = self.fetch_game_data(page)  # 获取游戏数据
        if not game_data:  # 如果没有数据，直接返回
            print(f"第 {page} 页没有数据")
            return
            
        for game in game_data:
            for player in game['players']:
                # 提取玩家信息
                nickname = player['nickname']  # 玩家昵称
                accountId = player['accountId']  # 玩家账户ID
                score = player['score']  # 玩家得分
                gradingScore = player['gradingScore']  # 玩家评分

                # 检查玩家是否已经处理过
                player_key = f"{accountId}_{game['uuid']}"
                if player_key in self.seen_players:
                    continue
                self.seen_players.add(player_key)

                player_records = self.fetch_player_records(accountId, gradingScore)  # 获取玩家记录
                for record in player_records:
                    # 只检查当前玩家的记录
                    for rec_player in record["players"]:
                        if rec_player['accountId'] == accountId:  # 只匹配当前玩家
                            item = {
                                'name': f"{nickname}[{score}]",  # 使用原始得分
                                'detail_url': f'https://game.maj-soul.com/1/?paipu={record["uuid"]}'  # 生成详情链接
                            }
                            print(item)  # 打印条目
                            self.save_to_csv(item)  # 保存条目到CSV文件
                            break  # 找到当前玩家后就跳出循环
        
        print(f"第 {page} 页处理完成")

    def save_to_csv(self, item):
        item_key = (item['name'], item['detail_url'])
        if item_key not in self.seen_items:
            self.seen_items.add(item_key)
            self.items_buffer.append(item)
            
            # 当缓冲区达到指定大小时，批量写入文件
            if len(self.items_buffer) >= self.buffer_size:
                try:
                    with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        if f.tell() == 0:
                            writer.writerow(self.fields)
                        for buffered_item in self.items_buffer:
                            writer.writerow([buffered_item[field] for field in self.fields])
                    print(f"成功批量保存 {len(self.items_buffer)} 条数据到CSV")
                    self.items_buffer = []  # 清空缓冲区
                except Exception as e:
                    print(f"保存数据时出错: {str(e)}")

    def flush_buffer(self):
        # 将缓冲区中剩余的数据写入文件
        if self.items_buffer:
            try:
                with open(self.csv_path, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    if f.tell() == 0:
                        writer.writerow(self.fields)
                    for buffered_item in self.items_buffer:
                        writer.writerow([buffered_item[field] for field in self.fields])
                print(f"成功保存剩余 {len(self.items_buffer)} 条数据到CSV")
                self.items_buffer = []
            except Exception as e:
                print(f"保存剩余数据时出错: {str(e)}")

    def run(self):
        start_time = time.time()
        
        # 根据页数范围动态设置线程数
        total_pages = self.end_page - self.start_page + 1
        max_workers = min(3, total_pages)  # 最多3个线程，但不超过总页数
        
        print(f"使用 {max_workers} 个线程处理 {total_pages} 页数据...")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(self.process_page, range(self.start_page, self.end_page + 1))
        
        # 确保所有缓冲数据都被保存
        self.flush_buffer()
        
        end_time = time.time()
        run_time = end_time - start_time
        
        # 获取保存的数据条数
        total_items = len(self.seen_items)
        
        # 打印完成信息
        print("\n" + "="*50)
        print(f"爬虫任务完成！")
        print(f"模式: {self.mode_names.get(self.mode, '未知')}")
        print(f"页数范围: {self.start_page}-{self.end_page}")
        print(f"实际处理页数: {len(self.processed_pages)}")
        print(f"日期: {self.date_str}")
        print(f"保存文件: {os.path.basename(self.csv_path)}")
        print(f"总计保存数据: {total_items} 条")
        print(f"运行时间: {run_time:.2f} 秒")
        print("="*50)

# 示例用法
if __name__ == '__main__':
    mode = 12  # 设置模式 王座: 16, 玉: 12, 金: 9, 王朝: 15, 玉东11, 金东: 8
    start_page = 1  # 设置起始页
    end_page = 4  # 设置结束页
    date_str = '2025-04-08'  # 设置日期
    scraper = MahjongScraper(mode, start_page, end_page, date_str)  # 创建爬虫实例
    scraper.run()  # 运行爬虫