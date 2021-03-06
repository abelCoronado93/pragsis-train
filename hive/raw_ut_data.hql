SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE EXTERNAL TABLE IF NOT EXISTS grupo01.ut_data
(Timestamp BIGINT,
PLC_CCTVyDILAX INT,
PLC_DesbloqPuM1R1 INT,
PLC_DesbloqPuM2R2 INT,
HMI1_LifeW INT,
HMI1_StsW INT,
HMI1_EBTestW INT,
HMI1_CMD01 INT,
HMI1_CMD02 INT,
HMI1_CMD03 INT,
HMI1_CMD04 INT,
HMI1_CMD05 INT,
HMI1_CMD06 INT,
HMI1_CMD07 INT,
HMI1_CMD08 INT,
HMI1_CMD09 INT,
HMI1_CMD10 INT,
HMI1_CMD11 INT,
HMI1_CMD12 INT,
HMI1_CMD13 INT,
HMI1_CMD14 INT,
HMI1_CMD15 INT,
HMI1_CMD16 INT,
HMI1_CMD17 INT,
HMI1_CMD18 INT,
HMI1_CMD19 INT,
HMI1_CMD20 INT,
HMI1_BYP_PRL_00 INT,
HMI1_BYP_PRL_01 INT,
HMI1_BYP_PRL_02 INT,
HMI1_BYP_PRL_03 INT,
HMI1_BYP_PRL_04 INT,
HMI1_BYP_PRL_05 INT,
HMI1_BYP_PRL_06 INT,
HMI1_BYP_PRL_07 INT,
HMI1_BYP_PRL_08 INT,
HMI1_BYP_PRL_09 INT,
HMI1_BYP_PRL_10 INT,
HMI1_BYP_PRL_11 INT,
HMI1_BYP_PRL_12 INT,
HMI1_BYP_PRL_13 INT,
HMI1_BYP_PRL_14 INT,
HMI1_BYP_PRL_15 INT,
HMI1_BYP_PRL_16 INT,
HMI1_BYP_PRL_17 INT,
HMI1_BYP_PRL_18 INT,
HMI1_BYP_PRL_19 INT,
HMI1_BYP_PRL_20 INT,
HMI1_ResetPow INT,
HMI1_DownloadStop INT,
HMI1_CMD21 INT,
HMI1_CMD22 INT,
HMI1_CMD23 INT,
HMI1_CMD24 INT,
HMI1_CMD25 INT,
HMI1_CMD26 INT,
HMI1_CMD27 INT,
HMI1_CMD28 INT,
HMI1_CMD29 INT,
HMI1_CMD30 INT,
HMI1_CMD31 INT,
HMI1_CMD32 INT,
HMI1_CMDMA06 INT,
HMI1_CMDMA07 INT,
HMI1_CMDMA08 INT,
HMI1_CMDMA09 INT,
HMI1_CMDMA10 INT,
HMI1_BYP_EBL_00 INT,
HMI1_BYP_EBL_01 INT,
HMI1_BYP_EBL_02 INT,
HMI1_BYP_EBL_03 INT,
HMI1_BYP_EBL_04 INT,
HMI1_BYP_EBL_05 INT,
HMI1_BYP_EBL_06 INT,
HMI1_BYP_EBL_07 INT,
HMI1_BYP_EBL_08 INT,
HMI1_BYP_EBL_09 INT,
HMI1_BYP_EBL_10 INT,
HMI1_BYP_EBL_11 INT,
HMI1_BYP_EBL_12 INT,
HMI1_BYP_EBL_13 INT,
HMI1_BYP_EBL_14 INT,
HMI1_BYP_EBL_15 INT,
HMI1_BYP_EBL_16 INT,
HMI1_BYP_EBL_17 INT,
HMI1_BYP_EBL_18 INT,
HMI1_BYP_EBL_19 INT,
HMI1_BYP_EBL_20 INT,
HMI1_CMDMA01 INT,
HMI1_CMDMA02 INT,
HMI1_CMDMA03 INT,
HMI1_CMDMA04 INT,
HMI1_CMDMA05 INT,
HMI1_ResetComp INT,
HMI1_CMD33 INT,
HMI1_SwV_Mode INT,
HMI1_ButIsolPanto1 INT,
HMI1_ButIsolPanto2 INT,
HMI1_ButIsolDisy1 INT,
HMI1_ButIsolDisy2 INT,
HMI1_ButIsolTCU1M1 INT,
HMI1_ButIsolTCU1M2 INT,
HMI1_ButIsolTCU2M1 INT,
HMI1_ButIsolTCU2M2 INT,
HMI1_ButIsolEpacM1 INT,
HMI1_ButIsolEpacM2 INT,
HMI1_ButIsolEpacR1 INT,
HMI1_ButIsolEpacR2 INT,
HMI1_CMD34 INT,
HMI1_CMD35 INT,
HMI1_CMD36 INT,
HMI1_CMD37 INT,
HMI1_CodeAlarm INT,
HMI1_Buzzer INT,
HMI1_ButIsolDisy1_REMOTE INT,
HMI1_ButIsolDisy2_REMOTE INT,
HMI1_ButIsolEpacM1_REMOTE INT,
HMI1_ButIsolEpacM2_REMOTE INT,
HMI1_ButIsolEpacR1_REMOTE INT,
HMI1_ButIsolEpacR2_REMOTE INT,
HMI1_ButIsolPanto1_REMOTE INT,
HMI1_ButIsolPanto2_REMOTE INT,
HMI1_ButIsolTCU1M1_REMOTE INT,
HMI1_ButIsolTCU1M2_REMOTE INT,
HMI1_ButIsolTCU2M1_REMOTE INT,
HMI1_ButIsolTCU2M2_REMOTE INT,
HMI1_CMD38 INT,
HMI1_CMD39 INT,
HMI1_CMD40 INT,
HMI1_CMD41 INT,
HMI1_CMD42 INT,
HMI1_CMD43 INT,
HMI1_CMD44 INT,
HMI1_CMD45 INT,
HMI1_GW_IsActive INT,
HMI1_GW_LifeWord INT,
HMI2_LifeW INT,
HMI2_StsW INT,
HMI2_EBTestW INT,
HMI2_CMD01 INT,
HMI2_CMD02 INT,
HMI2_CMD03 INT,
HMI2_CMD04 INT,
HMI2_CMD05 INT,
HMI2_CMD06 INT,
HMI2_CMD07 INT,
HMI2_CMD08 INT,
HMI2_CMD09 INT,
HMI2_CMD10 INT,
HMI2_CMD11 INT,
HMI2_CMD12 INT,
HMI2_CMD13 INT,
HMI2_CMD14 INT,
HMI2_CMD15 INT,
HMI2_CMD16 INT,
HMI2_CMD17 INT,
HMI2_CMD18 INT,
HMI2_CMD19 INT,
HMI2_CMD20 INT,
HMI2_BYP_PRL_00 INT,
HMI2_BYP_PRL_01 INT,
HMI2_BYP_PRL_02 INT,
HMI2_BYP_PRL_03 INT,
HMI2_BYP_PRL_04 INT,
HMI2_BYP_PRL_05 INT,
HMI2_BYP_PRL_06 INT,
HMI2_BYP_PRL_07 INT,
HMI2_BYP_PRL_08 INT,
HMI2_BYP_PRL_09 INT,
HMI2_BYP_PRL_10 INT,
HMI2_BYP_PRL_11 INT,
HMI2_BYP_PRL_12 INT,
HMI2_BYP_PRL_13 INT,
HMI2_BYP_PRL_14 INT,
HMI2_BYP_PRL_15 INT,
HMI2_BYP_PRL_16 INT,
HMI2_BYP_PRL_17 INT,
HMI2_BYP_PRL_18 INT,
HMI2_BYP_PRL_19 INT,
HMI2_BYP_PRL_20 INT,
HMI2_ResetPow INT,
HMI2_DownloadStop INT,
HMI2_CMD21 INT,
HMI2_CMD22 INT,
HMI2_CMD23 INT,
HMI2_CMD24 INT,
HMI2_CMD25 INT,
HMI2_CMD26 INT,
HMI2_CMD27 INT,
HMI2_CMD28 INT,
HMI2_CMD29 INT,
HMI2_CMD30 INT,
HMI2_CMD31 INT,
HMI2_CMD32 INT,
HMI2_CMDMA06 INT,
HMI2_CMDMA07 INT,
HMI2_CMDMA08 INT,
HMI2_CMDMA09 INT,
HMI2_CMDMA10 INT,
HMI2_BYP_EBL_00 INT,
HMI2_BYP_EBL_01 INT,
HMI2_BYP_EBL_02 INT,
HMI2_BYP_EBL_03 INT,
HMI2_BYP_EBL_04 INT,
HMI2_BYP_EBL_05 INT,
HMI2_BYP_EBL_06 INT,
HMI2_BYP_EBL_07 INT,
HMI2_BYP_EBL_08 INT,
HMI2_BYP_EBL_09 INT,
HMI2_BYP_EBL_10 INT,
HMI2_BYP_EBL_11 INT,
HMI2_BYP_EBL_12 INT,
HMI2_BYP_EBL_13 INT,
HMI2_BYP_EBL_14 INT,
HMI2_BYP_EBL_15 INT,
HMI2_BYP_EBL_16 INT,
HMI2_BYP_EBL_17 INT,
HMI2_BYP_EBL_18 INT,
HMI2_BYP_EBL_19 INT,
HMI2_BYP_EBL_20 INT,
HMI2_CMDMA01 INT,
HMI2_CMDMA02 INT,
HMI2_CMDMA03 INT,
HMI2_CMDMA04 INT,
HMI2_CMDMA05 INT,
HMI2_ResetComp INT,
HMI2_CMD33 INT,
HMI2_SwV_Mode INT,
HMI2_ButIsolPanto1 INT,
HMI2_ButIsolPanto2 INT,
HMI2_ButIsolDisy1 INT,
HMI2_ButIsolDisy2 INT,
HMI2_ButIsolTCU1M1 INT,
HMI2_ButIsolTCU1M2 INT,
HMI2_ButIsolTCU2M1 INT,
HMI2_ButIsolTCU2M2 INT,
HMI2_ButIsolEpacM1 INT,
HMI2_ButIsolEpacM2 INT,
HMI2_ButIsolEpacR1 INT,
HMI2_ButIsolEpacR2 INT,
HMI2_CMD34 INT,
HMI2_CMD35 INT,
HMI2_CMD36 INT,
HMI2_CMD37 INT,
HMI2_CodeAlarm INT,
HMI2_Buzzer INT,
HMI2_ButIsolDisy1_REMOTE INT,
HMI2_ButIsolDisy2_REMOTE INT,
HMI2_ButIsolEpacM1_REMOTE INT,
HMI2_ButIsolEpacM2_REMOTE INT,
HMI2_ButIsolEpacR1_REMOTE INT,
HMI2_ButIsolEpacR2_REMOTE INT,
HMI2_ButIsolPanto1_REMOTE INT,
HMI2_ButIsolPanto2_REMOTE INT,
HMI2_ButIsolTCU1M1_REMOTE INT,
HMI2_ButIsolTCU1M2_REMOTE INT,
HMI2_ButIsolTCU2M1_REMOTE INT,
HMI2_ButIsolTCU2M2_REMOTE INT,
HMI2_CMD38 INT,
HMI2_CMD39 INT,
HMI2_CMD40 INT,
HMI2_CMD41 INT,
HMI2_CMD42 INT,
HMI2_CMD43 INT,
HMI2_CMD44 INT,
HMI2_CMD45 INT,
HMI2_GW_IsActive INT,
HMI2_GW_LifeWord INT,
SI_LifeW INT,
SI_SIV_StsW INT,
SI_SVE_StsW INT,
SI_SIV_Int_Tir INT,
SI_Connex INT,
SI_SIV_NItinerary INT,
SI_SIV_NService INT,
SI_SIV_NDriver INT,
SI_SIV_PT INT,
SI_SIV_EstAct INT,
SI_SIV_EstSig INT,
SI_UC_SYNC_RESOURCES_STS INT,
SI_SIV_LubType INT,
SI_SIV_NItinerary_Origin INT,
SI_SIV_NService_Origin INT,
SI_SIV_NDriver_Origin INT,
SI_SIV_NItinerary_CV INT,
SI_SIV_NService_CV INT,
SI_SIV_NDriver_CV INT,
SI_SIV_PT_CV INT,
SI_SVE_HDServWeb INT,
SI_SVE_HDServMedios INT,
SDI_M1_M_Sts1W INT,
SDI_M1_M_Sts2W INT,
SDI_M1_M_Sts3W INT,
SDI_M1_M_DigInp INT,
SDI_M1_M_DigOut INT,
SDI_M1_M1_AlarmDet INT,
SDI_M1_R1_AlarmDet INT,
SDI_M1_R2_AlarmDet INT,
SDI_M1_M2_AlarmDet INT,
SDI_M1_M1_FailDet INT,
SDI_M1_R1_FailDet INT,
SDI_M1_R2_FailDet INT,
SDI_M1_M2_FailDet INT,
SDI_M1_M1_SwV INT,
SDI_M1_R1_SwV INT,
SDI_M1_R2_SwV INT,
SDI_M1_M2_SwV INT,
SDI_M1_CV INT,
SDI_M2_M_Sts1W INT,
SDI_M2_M_Sts2W INT,
SDI_M2_M_Sts3W INT,
SDI_M2_M_DigInp INT,
SDI_M2_M_DigOut INT,
SDI_M2_M1_AlarmDet INT,
SDI_M2_R1_AlarmDet INT,
SDI_M2_R2_AlarmDet INT,
SDI_M2_M2_AlarmDet INT,
SDI_M2_M1_FailDet INT,
SDI_M2_R1_FailDet INT,
SDI_M2_R2_FailDet INT,
SDI_M2_M2_FailDet INT,
SDI_M2_M1_SwV INT,
SDI_M2_R1_SwV INT,
SDI_M2_R2_SwV INT,
SDI_M2_M2_SwV INT,
SDI_M2_CV INT)
PARTITIONED BY (ut int, year int, month int, day int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE
LOCATION '/user/master/grupo1/raw'
tblproperties ("skip.header.line.count"="1");