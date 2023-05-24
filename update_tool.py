# --- coding: utf-8 ---
import json
import os
import sys
import time
from datetime import datetime

import boto3
import pandas as pd


class Update:

    LAMBDA_FUNCTION_NAME = f''
    TABLE_NAME           = f''
    DIVIDED_NUM          = 10

    def __init__(self):

        # テーブル名
        dynamodb = boto3.resource('dynamodb')
        self._table = dynamodb.Table(self.TABLE_NAME)

        # 端数の処理を通ったかどうかのフラグ
        self._change_count_flag       = False
        self._tsv_file                = ''
        self._log_name                = ''

    def exec(self):
        """ 更新実行
        Returns : (void)
        """

        print('開始')


        # ログファイル
        now_time = datetime.now()
        self._log_name = f'{self._tsv_file.replace(".tsv", "")}_{now_time.strftime("%Y%m%d%H%M%S")}.log'
        df = pd.read_table(self._tsv_file, dtype=str, skiprows=0, header=[1])
        # TSVの全件数
        total_count = len(df)

        # TSVを辞書に変更
        d_dict = df.to_dict('records')
        input_data_list = []
        count = 0

        for row in d_dict:

            # nanの場合は、削除として更新するため空文字に置き換え
            for key, item in row.items():
                row[key] = '' if str(item) == 'nan' else item

            input_data_list.append(json.dumps(row))

            # 端数が残った場合にカウントが最終的に特定件数になるように合わせる（1回のみ）
            if total_count < self.DIVIDED_NUM and not self._change_count_flag:
                count = self.DIVIDED_NUM - total_count
                self._change_count_flag = True

            count += 1

            # 特定の件数ずつ処理を行う
            if count % self.DIVIDED_NUM == 0:
                self._update_items(input_data_list)
                # 全件数から実行した件数をマイナス
                total_count -= self.DIVIDED_NUM
                print(f'残件数：{total_count if total_count > 0 else 0}')
                # リスト初期化
                input_data_list = []

    def _update_items(self, input_data_list):
        """ 更新処理
        Args:
            input_data_list:
        Returns: (void)
        """

        # リクエスト用のパラメータ
        request_param = {'param': {input_data_list}}
        request_json  = json.dumps(request_param)

        # lambda実行
        client = boto3.client('lambda')
        response = client.invoke(
            FunctionName=self.LAMBDA_FUNCTION_NAME,
            InvocationType='RequestResponse',
            Payload=request_json
        )

        output = response['Payload'].read().decode('utf-8')
        output = json.loads(json.dumps(json.loads(output), ensure_ascii=False))

        # レスポンスログ出力
        with open(self._log_name, 'a', encoding="utf-8") as f:
            for data in output:
                f.write("%s\n" % data)


# 処理開始時間
start = time.time()
update = Update()
update.exec()

# 処理完了時間
print(str(time.time() - start) + '秒')
print('完了しました')
