Copyright 2026 Acceldata Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

-----------------------------------------------------------------------
Why this directory has no *-changes.cfg
-----------------------------------------------------------------------

For an in-process plugin such as YARN or Hive, disabling means reverting
one property in the service's own site XML -- yarn-site.xml goes back to
its stock authorizer -- and enable-agent.sh does that by applying the
*-changes.cfg files found here.

Nothing equivalent exists for Airflow. Two things have to be undone and
neither is an XML property this installer can reach:

  1. [core] auth_manager in airflow.cfg must go back to the FAB auth
     manager. airflow.cfg is managed through Ambari, so a script editing
     it directly would be reverted on the next restart. It has to be
     changed through Ambari configuration.

  2. The agent process has to be stopped, and it is a separate daemon
     rather than a library inside a running service.

Both belong to disable-airflow-plugin.sh and the service-unit work, not
to conf templating. This file exists so the directory survives packaging
and so the omission reads as a decision rather than an oversight.

The agent's own XML (ranger-airflow-security.xml, ranger-airflow-audit.xml)
is deliberately left in place on disable: it is only read by the agent,
and once the agent is stopped and Airflow is back on FAB, nothing loads
it. Leaving it makes re-enabling cheap.
