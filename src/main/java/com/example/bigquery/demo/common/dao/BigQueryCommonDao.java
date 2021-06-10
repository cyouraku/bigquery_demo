package com.example.bigquery.demo.common.dao;

import java.util.UUID;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;


/**
 * BigQuery操作向け共通DAOクラス
 * @author Cyouraku
 *
 */
public class BigQueryCommonDao {

	/*** 共通BigQueryインスタンス ***/
	private BigQuery bigquery;

	/*** 内部変数「queryJobConfig」 ***/
	private QueryJobConfiguration queryJobConfig;

	/*** 内部変数「jobId」 ***/
	private JobId jobId;

	/*** 内部変数「queryJob」 ***/
	private Job queryJob;

	/*** 内部変数「bigQueryCommonDao」 ***/
	private volatile static BigQueryCommonDao bigQueryCommonDao;

	/**
	 * コンストラクタ
	 * 引数あり
	 * JobQueryConfigを設定する
	 * JobIdを設定する
	 * BigQueryを設定する
	 * QueryJobを設定する
	 */
	private BigQueryCommonDao(String sql, String projectId){
		//内部変数「queryJobConfig」を設定する
		if(this.queryJobConfig == null) {
			this.setJobQueryConfig(sql);
		}
		//内部変数「jobId」を設定する
		if(this.jobId == null) {
			this.setJobId();
		}
		//内部変数「bigquery」を設定する
		if(this.bigquery == null) {
			this.setBigQuery(projectId);
		}
		//内部変数「queryJob」を設定する
		if(this.queryJob == null) {
			this.setQueryJob();
		}
	}

	/**
	 * BigQueryCommonDaoのシングルインスタンスを取得する（同期ロックあり）
	 * @param sql
	 * @return
	 */
	public static BigQueryCommonDao getSingletonBigQueryCommonDao(String sql, String projectId) {
		if(bigQueryCommonDao == null) {
			synchronized(BigQueryCommonDao.class) {
				if(bigQueryCommonDao == null) {
					bigQueryCommonDao = new BigQueryCommonDao(sql, projectId);
				}
			}
		}
		return bigQueryCommonDao;
	}

	/**
	 * BigQueryCommonDaoのシングルインスタンスをクリアする。
	 */
	public static void clearBigQueryDao() {
		bigQueryCommonDao = null;
	}

	/**
	 * bigqueryを設定する
	 */
	public void setBigQuery(String projectId) {
		this.bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().getService();
	}

	/**
	 * bigqueryを取得する
	 * @return
	 */
	public BigQuery getBigQuery() {
		if(this.bigquery != null) {
			return this.bigquery;
		}
		return null;
	}

	/**
	 * QueryJobConfigurationを設定する
	 * @param sql
	 */
	public void setJobQueryConfig(String sql) {
		this.queryJobConfig = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build();
	}

	/**
	 * QueryJobConfigurationを取得する
	 * @return
	 */
	public QueryJobConfiguration getQueryJobConfig() {
		if(this.queryJobConfig != null) {
			return this.queryJobConfig;
		}
		return null;
	}

	/**
	 * jobIdを設定する
	 */
	public void setJobId() {
		this.jobId = JobId.of(UUID.randomUUID().toString());
	}

	/**
	 * jobIdを取得する
	 * @return
	 */
	public JobId getJobId() {
		if(this.jobId != null) {
			return this.jobId;
		}
		return null;
	}


	/**
	 *  queryJobを設定する
	 */
	public void setQueryJob() {
		if(this.queryJobConfig != null && this.jobId != null) {
			this.queryJob = bigquery.create(JobInfo.newBuilder(this.queryJobConfig).setJobId(this.jobId).build());
		}
	}

	/**
	 * queryJobを取得する
	 * @return
	 */
	public Job getQueryJob() {
		if(this.queryJob != null) {
			return this.queryJob;
		}
		return null;
	}

}
