REGISTER phw.jar;
REGISTER piggybank-0.12.0-cdh5.16.2.jar;
REGISTER cdh5-pig12-orcstorage-1.0.3.jar;

SET job.priority 'very_high';
SET mapred.child.java.opts '-Xmx1300m';

DEFINE BrowserValidator com.ttacross.phw.pig.BrowserValidator;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- CSV Parsing
--Timestamp,pId,RootDomain,lexId,ipAddress,usPrivacy,dnt,PartnerUidsCount,ReqFilterReasons,Country,Region,DeviceType,Browser,BrowserVersion,HasCookie,Source
--1652130378608,90,flamenipper.33across.com,g97412a7ad9992,162.213.133.199,1YN,0,1,,US,NY,desktop,Chrome,68.0,false,prebid-uim

input_data = LOAD '$inputdir' USING CSVExcelStorage() as (utc_time, pid, root_domain, lex_id, ip_address, us_privacy, dnt, partner_uids_count, req_filter_reasons, country, region, device_type, browser, browser_version, has_cookie, source, host_name, data_center);

filter_transactions = filter input_data by req_filter_reasons != 'BOUNCE' and ( pid is not null and TRIM(pid) != '' );

transactions = FOREACH filter_transactions GENERATE
  utc_time,
  (chararray)pid,
  (chararray)root_domain,
  (chararray)lex_id,
  ip_address,
  us_privacy,
  dnt,
  (partner_uids_count IS NULL ? 0 : partner_uids_count) as partner_uids_count,
  (((req_filter_reasons IS NULL or TRIM(req_filter_reasons) == '') and (partner_uids_count > 0)) ? 1 : 0) as response_count,
  (chararray)req_filter_reasons,
  ((req_filter_reasons IS NULL or TRIM(req_filter_reasons) == '') ? 1 : 0) as req_filter_reasons_count ,
  (chararray)country,
  region,
  (chararray)device_type as device,
  BrowserValidator((charArray)browser) as browser,
  browser_version,
  ((has_cookie IS NULL or TRIM(has_cookie) == '') ? 'U' : (has_cookie == true ? 'C' : 'CL')) as cookie_state,
  (chararray)source,
  '$inputtime' as qtime;

--Group by lex_id with other dimentions to calculate uniq parterids and total partner uids
trans_lexid_group = GROUP transactions BY (qtime, pid,root_domain,lex_id,country,device,browser,cookie_state,source,req_filter_reasons);

lexid_transactions = foreach trans_lexid_group generate flatten(group), COUNT(transactions) as total_requests, SUM(transactions.req_filter_reasons_count) as total_eligible_requests, SUM(transactions.response_count) as total_responses, MAX(transactions.partner_uids_count) as uniq_partner_ids, SUM(transactions.partner_uids_count) as total_partner_uids_count;

opportunities_group = GROUP lexid_transactions BY (qtime, pid,root_domain,country,device,browser,cookie_state,source,req_filter_reasons);
--
opportunity_count = foreach opportunities_group generate flatten(group), SUM(lexid_transactions.total_requests) as total_requests, SUM(lexid_transactions.total_eligible_requests)  as total_eligible_requests, SUM(lexid_transactions.total_responses)  as total_responses, SUM(lexid_transactions.uniq_partner_ids)  as uniq_partner_ids, SUM(lexid_transactions.total_partner_uids_count)  as total_partner_uids_count;

STORE opportunity_count INTO '$outputdir' USING OrcStorage();
--dump opportunity_count;
