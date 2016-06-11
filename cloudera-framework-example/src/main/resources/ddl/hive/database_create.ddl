--
-- Database grants and creates
--

CREATE ROLE ${hivevar:my.user};
GRANT ALL ON SERVER ${hivevar:my.server.name} TO ROLE ${hivevar:my.user};
GRANT ALL ON DATABASE ${hivevar:my.database.name} TO ROLE ${hivevar:my.user};
GRANT ROLE ${hivevar:my.user} TO GROUP ${hivevar:my.user};

CREATE DATABASE IF NOT EXISTS ${hivevar:my.database.name}
LOCATION '${hivevar:my.database.location}';
