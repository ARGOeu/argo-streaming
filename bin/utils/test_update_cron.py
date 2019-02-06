import unittest
import os
from update_cron import get_daily, get_hourly, gen_entry, gen_batch_ar, gen_batch_status, gen_tenant_all, gen_for_all
from argo_config import ArgoConfig

CONF_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../conf/argo-streaming.conf'))
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), '../../conf/config.schema.json')

# relative path to ar job submit script
BATCH_AR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../ar_job_submit.py'))
# relative path to status job submit script
BATCH_STATUS = os.path.abspath(os.path.join(os.path.dirname(__file__), '../status_job_submit.py'))
# bash argument to get today's date (utc)
TODAY = """$(/bin/date --utc +\%Y-\%m-\%d)"""
# bash argument to get previous date date (utc)
YESTERDAY = """$(/bin/date --utc --date '-1 day'  +\%Y-\%m-\%d)"""


class TestClass(unittest.TestCase):

    def test_update_cron(self):

        config = ArgoConfig(CONF_FILE, SCHEMA_FILE)

        # Test get_hourly
        self.assertEquals("8 * * * *", get_hourly(8))
        self.assertEquals("44 * * * *", get_hourly(44))
        self.assertEquals("32 * * * *", get_hourly(32))
        self.assertEquals("12 * * * *", get_hourly(12))
        self.assertEquals("5 * * * *", get_hourly())

        # Test get_daily
        self.assertEquals("8 1 * * *", get_daily(1, 8))
        self.assertEquals("44 3 * * *", get_daily(3, 44))
        self.assertEquals("32 4 * * *", get_daily(4, 32))
        self.assertEquals("12 5 * * *", get_daily(5, 12))
        self.assertEquals("0 5 * * *", get_daily())

        # Test gen_entry
        self.assertEquals("#simple command\n5 * * * * root echo 12\n",
                          gen_entry(get_hourly(), "echo 12", "root", "simple command"))

        self.assertEquals("#foo command\n8 12 * * * foo echo 1+1\n",
                          gen_entry(get_daily(12, 8), "echo 1+1", "foo", "foo command"))

        # Test generation of ar cronjob for a specific tenant and report
        expected = "#TENANT_A:report1 daily A/R\n"\
                   + "5 5 * * * foo " + BATCH_AR + " -t TENANT_A -r report1 -d " + YESTERDAY + " -m upsert " + "-c "\
                   + config.conf_path + "\n"

        self.assertEquals(expected, gen_batch_ar(config, "TENANT_A", "report1", "daily", "foo", "upsert"))

        # Test generation of ar cronjob for a specific tenant and report
        expected = "#TENANT_A:report1 hourly A/R\n"\
                   + "5 * * * * " + BATCH_AR + " -t TENANT_A -r report1 -d " + TODAY + " -m insert " + "-c "\
                   + config.conf_path + "\n"

        self.assertEquals(expected, gen_batch_ar(config, "TENANT_A", "report1", "hourly"))

        # Test generation of ar cronjob for a specific tenant and report
        expected = "#TENANT_B:report1 daily Status\n"\
                   + "5 5 * * * foo " + BATCH_STATUS + " -t TENANT_B -r report1 -d " \
                   + YESTERDAY + " -m upsert " + "-c "\
                   + config.conf_path + "\n"

        self.assertEquals(expected, gen_batch_status(config, "TENANT_B", "report1", "daily", "foo", "upsert"))

        # Test generation of status cronjob for a specific tenant and report
        expected = "#TENANT_B:report1 hourly Status\n"\
                   + "5 * * * * " + BATCH_STATUS + " -t TENANT_B -r report1 -d " + TODAY + " -m insert " + "-c "\
                   + config.conf_path + "\n"

        self.assertEquals(expected, gen_batch_status(config, "TENANT_B", "report1", "hourly"))

        # Test generation of cronjobs for a tenant's reports
        expected = "#Jobs for TENANT_A\n\n" \
                   + "#TENANT_A:report1 hourly A/R\n" \
                   + "5 * * * * " + BATCH_AR + " -t TENANT_A -r report1 -d " + TODAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report1 daily A/R\n" \
                   + "5 5 * * * " + BATCH_AR + " -t TENANT_A -r report1 -d " + YESTERDAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report1 hourly Status\n" \
                   + "5 * * * * " + BATCH_STATUS + " -t TENANT_A -r report1 -d " + TODAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report1 daily Status\n" \
                   + "5 5 * * * " + BATCH_STATUS + " -t TENANT_A -r report1 -d " + YESTERDAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report2 hourly A/R\n" \
                   + "5 * * * * " + BATCH_AR + " -t TENANT_A -r report2 -d " + TODAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report2 daily A/R\n" \
                   + "5 5 * * * " + BATCH_AR + " -t TENANT_A -r report2 -d " + YESTERDAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report2 hourly Status\n" \
                   + "5 * * * * " + BATCH_STATUS + " -t TENANT_A -r report2 -d " + TODAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "#TENANT_A:report2 daily Status\n" \
                   + "5 5 * * * " + BATCH_STATUS + " -t TENANT_A -r report2 -d " + YESTERDAY + " -m insert -c " \
                   + config.conf_path + "\n\n" \
                   + "\n"

        self.assertEquals(expected, gen_tenant_all(config, "TENANT_A"))

        # Test generation of cronjobs for all tenants and all reports
        expected2 = "#Jobs for TENANT_B\n\n" \
                    + "#TENANT_B:report1 hourly A/R\n" \
                    + "5 * * * * " + BATCH_AR + " -t TENANT_B -r report1 -d " + TODAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report1 daily A/R\n" \
                    + "5 5 * * * " + BATCH_AR + " -t TENANT_B -r report1 -d " + YESTERDAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report1 hourly Status\n" \
                    + "5 * * * * " + BATCH_STATUS + " -t TENANT_B -r report1 -d " + TODAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report1 daily Status\n" \
                    + "5 5 * * * " + BATCH_STATUS + " -t TENANT_B -r report1 -d " + YESTERDAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report2 hourly A/R\n" \
                    + "5 * * * * " + BATCH_AR + " -t TENANT_B -r report2 -d " + TODAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report2 daily A/R\n" \
                    + "5 5 * * * " + BATCH_AR + " -t TENANT_B -r report2 -d " + YESTERDAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report2 hourly Status\n" \
                    + "5 * * * * " + BATCH_STATUS + " -t TENANT_B -r report2 -d " + TODAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "#TENANT_B:report2 daily Status\n" \
                    + "5 5 * * * " + BATCH_STATUS + " -t TENANT_B -r report2 -d " + YESTERDAY + " -m insert -c " \
                    + config.conf_path + "\n\n" \
                    + "\n"

        expected = expected + expected2
        self.assertEquals(expected, gen_for_all(config))
