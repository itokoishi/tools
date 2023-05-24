# --- coding: utf-8 ---
import pandas as pd
from datetime import datetime
import boto3
import os
import sys


class Delete:

    TABLE      = 'table'

    def __init__(self):

        # dynamo実行
        dynamodb = boto3.resource('dynamodb')
        self._total_count = 0
        self._tsv_file    = ''
        self._table_name  = f'{self.TABLE}'
        self._log_name    = ''
        self._target_key  = ''
        self._target      = ''
        self._table       = dynamodb.Table(self._table_name)

    def exec(self):

        now_time = datetime.now()
        self._log_name = f'{self._tsv_file.replace(".tsv", "")}_{now_time.strftime("%Y%m%d%H%M%S")}.log'

        df = pd.DataFrame()
        d_dict = df.to_dict('records')

        # -------------------------------------
        # 実行開始
        # -------------------------------------
        print('開始')

        # -- ターゲット検索 ---------------------

        row_num = 1
        for row in d_dict:
            items = self._get_mst_itm_by_target(row)
            if len(items) == 0:
                row_num += 1
                continue

            # -- 削除実行---------------------
            if items:
                # 複数ある場合reg_dtが昇順になるようにソートして１件のみ削除
                item = sorted(items, key=lambda x: x['reg_dt'])
                pk   = item[0].get('PK')
                sk   = item[0].get('SK')
                self._delete(pk, sk)
                print(f'ターゲット {self._target}の削除完了')
                row_num += 1

    def _delete(self, pk, sk):
        """ データの削除
        Args:
            pk (str): プライマリキー
            sk (str): ソートキー
        """

        # PKに紐づく全てのレコード取得
        self._table.delete_item(Key={'PK': pk, 'SK': sk})

        # レスポンスログ出力
        with open(self._log_name, 'a', encoding="utf-8") as f:
            f.write("%s\n" % f'target:{self._target} PK: {pk}')


    def _get_mst_itm_by_target(self, row):
        """ ターゲットをもとに、データ取得
        Args:
            row (dict): TSVデータ
        Returns: (dict): データ
        """

        # targetをもとに、データ取得
        self._target = row.get(self._target_key)

        options = {
            'TableName'                : self._table_name,
            # 'IndexName'                : '',
            'KeyConditionExpression'   : '#target = :target',
            'ExpressionAttributeNames' : {'#target': self._target_key},
            'ExpressionAttributeValues': {':target': self._target},
            'ProjectionExpression'     : 'PK, SK'
        }
        response = self._table.query(**options)
        items    = response.get('Items')
        return items


delete = Delete()
delete.exec()
print('完了')
