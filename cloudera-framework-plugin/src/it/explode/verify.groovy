File parcel = new File( basedir, "../../local-repo/com/cloudera/parcel/explode/SQOOP_NETEZZA_CONNECTOR/1.3c5/SQOOP_NETEZZA_CONNECTOR-1.3c5-el6.parcel" );
File parcelSha1 = new File( basedir, "../../local-repo/com/cloudera/parcel/explode/SQOOP_NETEZZA_CONNECTOR/1.3c5/SQOOP_NETEZZA_CONNECTOR-1.3c5-el6.parcel.sha1" );
File parcelExploded = new File( basedir, "target/test-parcels/SQOOP_NETEZZA_CONNECTOR-1.3c5" );

assert parcel.isFile()
assert parcelSha1.isFile()
assert parcelExploded.isDirectory()

return true;
