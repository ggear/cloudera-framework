--
-- Database grants and creates
--

GRANT ALL ON SERVER ${hivevar:user.server} TO ROLE ${hivevar:user.admin};
GRANT ROLE ${hivevar:user.admin} TO GROUP ${hivevar:user.admin};

CREATE ROLE ${hivevar:user.app};
GRANT ALL ON DATABASE ${hivevar:database.name} TO ROLE ${hivevar:user.app};
GRANT ROLE ${hivevar:user.app} TO GROUP ${hivevar:user.app};

CREATE DATABASE IF NOT EXISTS ${hivevar:database.name} LOCATION '${hivevar:database.location}';
