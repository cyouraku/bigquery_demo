package com.example.bigquery.demo.common.dao;

import com.example.bigquery.demo.common.BigQueryInterface;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.QueryJobConfiguration;

/**
 * 保護された共通DAOクラス操作用抽象クラス
 * @author Cyouraku
 *
 */
public abstract class BigQueryAbstractDao implements BigQueryInterface {

	/** 改行コード */
	protected static final String LINE_SEPARATOR = System.getProperty("line.separator");

	/** 保護された共通DAOクラスBigQueryCommonDao **/
	protected BigQueryCommonDao dao;

	/** 保護されたBigQuery対象 **/
	protected BigQuery bigquery;

	/** 保護されたQueryJobConfiguration対象 **/
	protected QueryJobConfiguration queryConfig;

	/** 保護されたJob対象 **/
	protected Job queryJob;

	/**
	 * 「共通DAOクラスBigQueryCommonDao」を取得してローカルdao変数に設定する。
	 * @param sql
	 * @return
	 */
	protected BigQueryCommonDao getDao(String sql, String projectId) {
		this.dao = BigQueryCommonDao.getSingletonBigQueryCommonDao(sql, projectId);
		return this.dao;
	}

	/**
	 * 1.「共通DAOクラスBigQueryCommonDao」をクリアする。
	 * 2.ローカルdao変数をクリアする。
	 */
	protected void clearDao() {
		BigQueryCommonDao.clearBigQueryDao();
		this.dao = null;
	}

}
