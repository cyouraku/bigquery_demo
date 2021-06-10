package com.example.bigquery.demo.common;

import java.util.List;
import java.util.Map;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.QueryJobConfiguration;

/**
 * BigQueryを操作するメソッドを公開するインターフェース
 * @author Cyouraku
 *
 */
public interface BigQueryInterface {

	/**
	 * Sql文を作成する
	 * @param param
	 * @return
	 */
	String createSql(String... param);

	/**
	 * BigQueryにてSELECT処理を実行する
	 * @param bigquery
	 * @param queryConfig
	 * @return
	 */
	List<Map<String, Object>> selectQuery(BigQuery bigquery, QueryJobConfiguration queryConfig);

	/**
	 * BigQueryにてUPDATE処理を実行する
	 * @param queryJob
	 * @return
	 */
	boolean updateQuery(Job queryJob);

}
