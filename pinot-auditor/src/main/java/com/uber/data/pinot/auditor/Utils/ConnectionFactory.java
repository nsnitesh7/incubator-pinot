package com.uber.data.pinot.auditor.Utils;

import com.uber.data.pinot.auditor.Entities.TimeBucketFlattenedMetric;
import com.uber.data.pinot.auditor.Storage.TimeBucketMetricsDAO;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import java.util.List;

import static com.uber.data.pinot.auditor.Utils.Constants.*;

/**
 * Connect to Database
 */
public class ConnectionFactory {
    private static TimeBucketMetricsDAO dao = null;

    /**
     * Get a connection to database
     *
     * @return Jdbi object
     */
    public static TimeBucketMetricsDAO getTimeBucketMetricsDao() {
        if (dao == null) {
            dao = Jdbi.create(DATABASE_URL, USER, PASS)
                    .installPlugin(new SqlObjectPlugin())
                    .onDemand(TimeBucketMetricsDAO.class);
        }
        return dao;
    }

    /**
     * Test Connection
     */
    public static void main(String[] args) {
        TimeBucketMetricsDAO dao = getTimeBucketMetricsDao();

        TimeBucketFlattenedMetric metric = new TimeBucketFlattenedMetric("program2", "sjc", "topic2", 0, 0, 0, 0, System.currentTimeMillis(), System.currentTimeMillis(), 0, 0, 0, 0, 0, 0);
        dao.writeMetric(metric);
        List<TimeBucketFlattenedMetric> metrics = dao.getMetrics();
        metrics.size();
    }
}