#!/usr/bin/perl

print "\n========== xa_fields (XA3.pl) ==========\n\n";

use Time::Local;

$spath = "/home/scripts/XA";
$dpath = "/home/scripts/XA/DATA";
$jpath = "/home/scripts/XA/JSON";
$csvpath = "/home/scripts/XA/CSV";
$mpath = "/home/scripts/XA/MANIFESTS";
$cpath = "/home/scripts";

# credentials --------------------------------------------------
open(UP,"$cpath/U_P.txt") || die("UP file not found\n");
while($row=<UP>){
  @up=split(/,/,$row);
  $u=@up[0];
  $p=@up[1];
  chomp($p);
}
close(UP);

system("rm $jpath/*.json -f");

$rows = 1000000;

#$sql = "\"   SELECT * FROM meld.jrnl.v_virtualassistantsummary FETCH FIRST 100000 ROWS ONLY\"";
$sql = "\"   SELECT sessionid,starttimestamp,endtimestamp,accountnumber,region,division,authenticationtype,channel,fiscalmonthenddate,fiscalweekenddate,fullpenultimate,fullthemeid,fullpath,initialcontentcode,initialintentcode,virtualhold,sessiontype,truechatflag,chatflag,socialflag,callflag,chatflag7day,socialflag7day,callflag7day,chatflagsamequeue,socialflagsamequeue,callflagsamequeue,chatflag7daysamequeue,socialflag7daysamequeue,callflag7daysamequeue,contactflag,contactflag7day,uniquequeries,lob,callgroup,interactiongroup,interactiontype,didselfservice,didinfodelivery,didmessaging,didnavigation,endstatus,hasvideo,hashsd,hascdv,hasxh,platform,createtimestampepoch,id,statusdate,statustype,source_name,audit_dml_action_cd,audit_job_id,audit_inserted_ts,audit_updated_ts,current_record_ind,eff_dt,eff_ts,exp_ts,versionid,contentversion,unique15minsession,unique15mincontactflag,unique15mincontactflag7day,day_id FROM meld.jrnl.v_virtualassistantsummary FETCH FIRST $rows ROWS ONLY\"";

$port = "9443";
$nextUri = "https://query.comcast.com:$port/v1/statement";

$loop_count = 0;

system("curl -k -u $u:$p -X POST -L --cacert \"/home/sghiz/anaconda3/ssl/cert.pem\" -H \"X-Presto-User:$u\" -d $sql $nextUri?pretty > $jpath/$loop_count.json");

while (1) {
  $nextUri = "";
  open(TEMP,"$jpath/$loop_count.json") || die("cannot open $jpath/$loop_count.json\n");
  while($row=<TEMP>){
    chomp($row);
    if($row =~ m/nextUri/){
      $row =~ s/,//g;
      $row =~ s/ //g;
      $row =~ s/"//g;
      $row =~ s/nextUri://g;
      $nextUri = $row;
      print "$nextUri\n";
    }
  }
  close(TEMP);

  $loop_count++;

  if($loop_count == 100000000){
    last;
  } elsif($nextUri eq "") {
    print "nextUri is BLANK\n";
    last;
  }

  system("curl -k -u $u:$p -X GET  -H \"X-Presto-User:$u\" $nextUri?pretty > $jpath/$loop_count.json");
  close(SZ);
  sleep 2;
  print "loop count == $loop_count\n";

  system("cat $jpath/$loop_count.json $jpath/all7.json > $jpath/temp7.json");
  system("mv $jpath/temp7.json $jpath/all7.json");

}

$loop_count1 = $loop_count - 1;
open(HEAD,"$jpath/$loop_count1.json") || die("cannot open $jpath/$loop_count1.json\n");
while($row=<HEAD>){
  chomp($row);
  if($row =~ m/"name" :/){
    $row =~ s/\"//g;
    $row =~ s/name : //g;
    $row =~ s/,//g;
    $row =~ s/ //g;
    push @header, $row;
  }
}
close(HEAD);

open(FINAL,">","$jpath/final7.json") || die("cannot open $jpath/final7.json\n");
open(ALL,"$jpath/all7.json") || die("cannot open $jpath/all7.json\n");
while($row=<ALL>){
  chomp($row);
  if($row =~ m/\"data\"/){
    print FINAL "$row\n";
  }
}
close(ALL);
close(FINAL);

open(NEW,">","$csvpath/final7.csv") || die("cannot open $csvpath/final7.csv\n");
open(DATA,"$jpath/final7.json") || die("cannot open $jpath/final7.json\n");
print NEW join(',', @header), "\n";
while($row=<DATA>){
  chomp($row);
  $row =~ s/ \"data\" \: \[ //g;
  $row =~ s/ \] \]\,/ \]\,/g;
  $row =~ s/ \[ //g;
  $row =~ s/ \],/\n/g;
  #$row =~ s/\; null/\, null/g;
  $row =~ s/\", /<<>>/g;
  $row =~ s/, \"/<<>>/g;
  $row =~ s/, 0/<<>> 0/g;
  $row =~ s/, 1/<<>> 1/g;
  $row =~ s/, 2/<<>> 2/g;
  $row =~ s/, 3/<<>> 3/g;
  $row =~ s/, 4/<<>> 4/g;
  $row =~ s/, 5/<<>> 5/g;
  $row =~ s/, 6/<<>> 6/g;
  $row =~ s/, 7/<<>> 7/g;
  $row =~ s/, 8/<<>> 8/g;
  $row =~ s/, 9/<<>> 9/g;
  $row =~ s/, null/<<>> null/g;
  $row =~ s/\"//g;
  $row =~ s/\,/\;/g;
  $row =~ s/<<>>/,/g;
#  if($row !~ m/skill.xa4m.autopay.thankyou/ || $row !~ m/null/ || $row !~ m/skill.xrp.predictive.recommendation/ || $row !~ m/account-x1/){
    print NEW "$row";
#  }
}
close(DATA);
close(NEW);

########### EXIT ##############
exit;##########################
###############################

##########################################################################

