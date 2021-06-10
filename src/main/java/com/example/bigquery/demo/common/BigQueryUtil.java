package com.example.bigquery.demo.common;

import com.google.cloud.bigquery.FieldValue;

/**
 * BigQuery共通ユーティリティクラス
 * @author Cyouraku
 *
 */
public class BigQueryUtil {

	/**
	 * BigQueryのTableResult型のフィールド値がヌル値である場合は空白値を返す。
	 *
	 * @param inField
	 * @return
	 */
	public static Object isNull(FieldValue inField) {
		return inField.getValue() == null ? "-" : inField.getStringValue();
	}

}
