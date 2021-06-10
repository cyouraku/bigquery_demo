package com.example.bigquery.demo.business.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.bigquery.demo.common.BigQueryUtil;
import com.example.bigquery.demo.common.ConstBigQuery;
import com.example.bigquery.demo.common.dao.BigQueryAbstractDao;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;

/**
 * サンプルビジネス情報検索向けDAOクラス.
 * @author Cyouraku
 *
 */
public class SampleBusinessDao extends BigQueryAbstractDao {

	/**
	 * BigQuery検索結果を取得するサンプル
	 * @return
	 */
	public List<Map<String, Object>> searchDemo1() {
		//SQL文を生成する
		String sql = this.createSql("","","");

		//共通DAOクラスからBigQueryCommonDaoインスタンスを取得する。
		this.dao = this.getDao(sql, ConstBigQuery.ProjectId);
		//BigQueryインスタンスを取得する。
		this.bigquery = this.dao.getBigQuery();
		//QueryJobConfigインスタンスを取得する。
		this.queryConfig = this.dao.getQueryJobConfig();

		// BigQuery検索結果を取得
		List<Map<String, Object>> result = this.selectQuery(bigquery, queryConfig);

		//BigQueryCommonDaoインスタンスをクリアする
		this.clearDao();

		return result;
	}

	/**
	 * BigQuery検索結果から文字列を取得するサンプル
	 * @return
	 */
	public String searchDemo2() {
		//SQL文を生成する
		String sql = this.createSql("","","");

		//共通DAOクラスからBigQueryCommonDaoインスタンスを取得する。
		this.dao = this.getDao(sql, ConstBigQuery.ProjectId);
		//BigQueryインスタンスを取得する。
		this.bigquery = this.dao.getBigQuery();
		//QueryJobConfigインスタンスを取得する。
		this.queryConfig = this.dao.getQueryJobConfig();

		// BigQuery検索結果を取得
		List<Map<String, Object>> result = this.selectQuery(bigquery, queryConfig);

		//BigQueryCommonDaoインスタンスをクリアする
		this.clearDao();

		return this.getStringResult(result);
	}

	/**
	 * BigQuery検索結果から文字列を取得するサンプル
	 * @param result
	 * @return
	 */
	private String getStringResult(List<Map<String, Object>> result) {
		String str_ret = "";
		//例：BigQuery処理結果からカラム情報を取得する
		int max_row = 1000;
		for (int i = 0; i < max_row; i++) {
			Map<String, Object> map = result.get(i);
			//例：[品番]を取得する。
			str_ret = map.get("hinban").toString();
		}

		return str_ret;
	}


	@Override
	public String createSql(String... param) {
		return null;
	}

	@Override
	public List<Map<String, Object>> selectQuery(BigQuery bigquery, QueryJobConfiguration queryConfig) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		TableResult result = null;
		try {
			result = bigquery.query(queryConfig);
			for (FieldValueList row : result.iterateAll()) {
				Map<String, Object> map = new HashMap<String, Object>();
				//BigQueryのカラム名を設定する
				//フィールド値がヌル値である場合は空白値を返す。
				map.put("hinban", BigQueryUtil.isNull(row.get("hinban")));
				list.add(map);
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (JobException e1) {
			e1.printStackTrace();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		return list;
	}

	@Override
	public boolean updateQuery(Job queryJob) {
		return false;
	}

}
