# --- coding: utf-8 ---
import pandas as pd
import json
from datetime import datetime
import time
import boto3


class Insert:

    DIVIDED_NUM          = 10
    LAMBDA_FUNCTION_NAME = ''

    def __init__(self):
        self._change_count_flag       = False  # 端数の処理を通ったかどうかのフラグ
        self._tsv_file                = ''
        self._log_name                = ''

    def exec(self):
        """
        登録実行
        Returns: (void)
        """

        print('開始')

        # -- TSVファイル読み込み ---------------------
        df = pd.read_table(self._tsv_file, dtype=str, skiprows=0, header=[1])
        # TSVの全件数
        total_count = len(df)

        # ログファイル
        now_time = datetime.now()
        self._log_name = f'{self._tsv_file.replace(".tsv", "")}_{now_time.strftime("%Y%m%d%H%M%S")}.log'

        # TSVを辞書に変更
        count = 0
        d_dict = df.to_dict('records')
        input_data_list = []

        for row in d_dict:

            # nanは削除
            row = {k: v for k, v in row.items() if str(v) != 'nan'}
            input_data_list.append(row)

            # 端数が残った場合にカウントが最終的に特定件数になるように合わせる（1回のみ）
            if total_count < self.DIVIDED_NUM and not self._change_count_flag:
                count = self.DIVIDED_NUM  - total_count
                self._change_count_flag = True

            count += 1

            # 特定の件数ずつ処理を行う
            if count % self.DIVIDED_NUM == 0:
                # 登録
                self._insert_items(input_data_list)
                # 全件数から実行した件数をマイナス
                total_count -= self.DIVIDED_NUM
                print(f'残件数：{total_count if total_count > 0 else 0}')

                # 登録用のパラメータリスト初期化
                input_data_list = []

    def _insert_items(self, input_data_list):
        """ 新規登録(登録用のlambdaを使用する)
        Args:
            input_data_list:
        Returns: (void)
        """

        # リクエスト用のパラメータ
        request_param = {'param': input_data_list}
        request_json = json.dumps(request_param)

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


start = time.time()
insert = Insert()
insert.exec()

# 処理完了時間
print(str(time.time() - start) + '秒')
print('完了')
