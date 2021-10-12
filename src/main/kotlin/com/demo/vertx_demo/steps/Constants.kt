package com.demo.vertx_demo.steps

const val USER = "user"
const val USERNAME = "username"
const val ID = "_id"
const val PASSWORD = "password"
const val EMAIL = "email"
const val CITY = "city"
const val DEVICE_ID = "device_id"
const val MAKE_PUBLIC = "make_public"
const val DEVICE_SYNC = "device_sync"
const val STEPS_COUNT = "steps_count"
const val TIMESTAMP = "timestamp"

const val STEP_EVENTS_QUEUE = "step.events"
const val INCOMING_STEPS_TOPIC = "incoming.steps"
const val DAILY_STEP_UPDATES_TOPIC = "daily.step.updates"
const val EVENT_STATS_USER_ACTIVITY_UPDATES_TOPIC = "event-stats.user-activity.updates"
const val EVENT_STATS_THROUGHPUT_TOPIC = "event-stats.throughput"
const val EVENT_STATS_CITY_TREND_UPDATES_TOPIC = "event-stats.city-trend.updates"

const val APPLICATION_JSON = "application/json"
const val HTTP_PORT = "http.port"

const val PG_DUPLICATE_KEY_ERROR_CODE = "23505"
const val INSERT_STEP_EVENT_QUERY = "INSERT INTO stepevent VALUES(\$1, \$2, current_timestamp, $3)"

const val TODAYS_STEP_COUNT_QUERY = """
    SELECT current_timestamp, coalesce(sum(steps_count), 0) 
    FROM stepevent 
    WHERE (device_id = $1) AND (date_trunc('day', sync_timestamp) = date_trunc('day', current_timestamp))
    """

const val TOTAL_STEPS_COUNT_QUERY = "SELECT sum(steps_count) FROM stepevent WHERE (device_id = \$1)"

const val MONTHLY_STEPS_QUERY = "SELECT sum(steps_count) FROM stepevent WHERE (device_id = \$1) AND (date_trunc('month', sync_timestamp) = \$2::timestamp)"

const val DAILY_STEPS_QUERY = "SELECT sum(steps_count) FROM stepevent WHERE (device_id = \$1) AND (date_trunc('day', sync_timestamp) = \$2::timestamp)"

const val LAST_24_HOURS_RANKING_QUERY = """
    SELECT device_id, sum(steps_count) as steps
    FROM stepevent
    WHERE (now() - sync_timestamp <= (interval '24 hours'))
    GROUP BY device_id ORDER BY steps DESC
    """