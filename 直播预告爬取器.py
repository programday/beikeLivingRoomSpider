import requests, time, os, openpyxl
from datetime import datetime
from urllib3 import disable_warnings
from concurrent.futures import ThreadPoolExecutor

disable_warnings()


class BeiKeLiveRoom:
    cityCodes = {
        '苏州': {'parentSceneId': 6106123872458991617, 'cityId': 320500},
        '杭州': {'parentSceneId': 6106130410369857281, 'cityId': 330100},
        '广州': {'parentSceneId': 6106131227888678913, 'cityId': 440100},
        '成都': {'parentSceneId': 6106131840864297473, 'cityId': 510100},
        '合肥': {'parentSceneId': 6106134081175244545, 'cityId': 340100},
        '廊坊': {'parentSceneId': 6106134843280989697, 'cityId': 131000},
        '上海': {'parentSceneId': 6106135483146859265, 'cityId': 310000},
        '天津': {'parentSceneId': 6106136106639701505, 'cityId': 120000}
    }
    headers = {
        'Cookie': 'H5SceneFromNative=%7B%22sceneId%22%3A%226104731953736756993%22%7D; beikeBaseData=%7B%22duid%22%3A%22%22%2C%22fpid%22%3A%220201029Ie5PGjcoD1y9x%2F5oL5Q4Sm%2BkwZzB6zN%2F8xTr%2B7vzWC%2F6w3he1DzTwCrVI0Vkf%2FfxFPQUr%2BcJA9cL81ftKIcXw%5Cu003d%5Cu003d%22%2C%22appVersion%22%3A%222.79.0%22%7D; digData=%7B%22evt%22%3A%2241645%22%2C%22key%22%3A%22zb_pindao%22%2C%22ts%22%3A%221652705378235%22%7D; lianjia_ssid=0e5c59f3-9662-4086-9209-5c1d72213148; algo_session_id=c8dee080-b719-448c-93ac-e6bdd18e90c1; lianjia_token=; lianjia_udid=bd405b519c7eec1a; lianjia_uuid=118a1dc7-9e78-45b1-9f6f-e3e6468f8221; select_city=310000; staticData=%7B%22accessToken%22%3A%22%22%2C%22appName%22%3A%22%E8%B4%9D%E5%A3%B3%E6%89%BE%E6%88%BF%22%2C%22appVersion%22%3A%222.79.0%22%2C%22deviceId%22%3A%22bd405b519c7eec1a%22%2C%22deviceInfo%22%3A%7B%22ketoken%22%3A%22Hd1D1tRffaJw4kdYoUPBUibm18bqR6Pf94wnYIrq2HQXhTB08%2FITY094gfJ7EJBTm0vFeqp1hTU9MtW4zTjQi8JQrbDOKLBB%2FnNeE%2B6j79ccEg5Nru802Q%2F7TtBK3Ycz%22%2C%22ssid%22%3A%220e5c59f3-9662-4086-9209-5c1d72213148%22%2C%22udid%22%3A%22bd405b519c7eec1a%22%2C%22uuid%22%3A%22118a1dc7-9e78-45b1-9f6f-e3e6468f8221%22%7D%2C%22extraData%22%3A%7B%22cityId%22%3A%22310000%22%2C%22cityName%22%3A%22%E4%B8%8A%E6%B5%B7%22%2C%22ip%22%3A%22192.168.0.102%22%2C%22latitude%22%3A%224.9E-324%22%2C%22locationCityName%22%3A%22%22%2C%22longitude%22%3A%224.9E-324%22%2C%22wifiName%22%3A%22VMOSWIFI%22%7D%2C%22network%22%3A%22WIFI%22%2C%22packageName%22%3A%22Android_ke_baidupinzhuan_lp%22%2C%22scheme%22%3A%22lianjiabeike%22%2C%22sysModel%22%3A%22android%22%2C%22sysVersion%22%3A%22Android+7.1.2%22%2C%22userInfo%22%3A%7B%22userName%22%3A%22%22%7D%7D; lianjia_ssid=0e5c59f3-9662-4086-9209-5c1d72213148; lianjia_token=; lianjia_uuid=118a1dc7-9e78-45b1-9f6f-e3e6468f8221; lianjia_udid=bd405b519c7eec1a; select_city=310000; sajssdk_2015_cross_new_user=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22180cceac2571c-0a0d9d47601d2f-5d113128-285120-180cceac258727%22%2C%22%24device_id%22%3A%22180cceac2571c-0a0d9d47601d2f-5d113128-285120-180cceac258727%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_referrer_host%22%3A%22%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%7D%7D; beikeBaseData=%7B%22parentSceneId%22:%226104731953736756993%22%7D; fangtrade_tob=0',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1.2; OCE-AN10 Build/N6F26Q; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/81.0.4044.117 Mobile Safari/537.36/Lianjia/lianjiabeike/2.79.0;webank/h5face;webank/1.0;netType:NETWORK_WIFI;appVersion:2790100;packageName:com.lianjia.beike'
    }

    def __init__(self):
        self.url = 'https://fang-trade.ke.com/api/liveRoom/liveAdvanceList'
        self.day = datetime.now().strftime('%Y-%m-%d')
        self.excel_dirname = '直播预告数据'
        if not os.path.exists(f'./{self.excel_dirname}'):
            os.mkdir(f'./{self.excel_dirname}')

    def get_live_rooms(self, city, city_params):
        page, total_rooms = 1, []
        while 1:
            params = city_params.copy()
            params.update({'page': page, 'pagesize': 10, 'timeRange': 0})
            try:
                response = requests.get(
                    self.url, params=params, headers=self.headers, verify=False, timeout=10,
                    proxies={'http': None, 'https': None}
                )
                if response.status_code != 200:
                    print(f'获取{city}数据异常，即将重试')
                    time.sleep(2)
                    continue
            except:
                print(f'获取{city}数据异常，即将重试')
                time.sleep(2)
                continue

            rooms = response.json()['data']['data']['list']
            if not rooms:
                return self.save_to_excel(city, total_rooms)

            for time_rooms in rooms:
                for room in time_rooms['data']:
                    room_id = str(room['roomId'])           # 直播间ID
                    anchor_name = room['anchorName']        # 主播姓名
                    anchor_tag = room['hostTagText']    # 主播标签
                    live_start_time = datetime.fromtimestamp(room['announceStartTime']).strftime('%Y-%m-%d %H:%M')     # 直播开始时间

                    room_data = {
                        'room_id': room_id, 'spider_date': self.day,
                        'anchor_name': anchor_name, 'anchor_tag': anchor_tag,
                        'live_start_time': live_start_time
                    }

                    total_rooms.append(room_data)

            page += 1
            time.sleep(3)

    def save_to_excel(self, city, rooms_data):
        filename = f'./{self.excel_dirname}/{city}.xlsx'
        workbook = openpyxl.load_workbook(filename)
        worksheet = workbook.active
        
        exists_room_ids = [worksheet.cell(row=row, column=1).value for row in range(2, worksheet.max_row+1)]
        [worksheet.append(tuple(data.values())) for data in rooms_data if data['room_id'] not in exists_room_ids]

        try:
            workbook.save(filename)
        except PermissionError:
            print(f'请关闭 {city}.xlsx 后再爬取数据')

        workbook.close()

        print(f'{city}的直播预告已抓取完毕')

    def run(self):
        t = ThreadPoolExecutor(max_workers=2)
        tasks = [t.submit(self.get_live_rooms, city, params) for city, params in self.cityCodes.items()]
        for task in tasks:
            task.result()

        print('全部城市的今日直播预告已全部爬取完毕, 请在文件夹中查看Excel\n')
        os.system('pause')


if __name__ == '__main__':
    BeiKeLiveRoom().run()
