create table yearly_fact
(
    Tahun                    bigint null,
    kode_prov                text   null,
    nama_prov                text   null,
    SUSPECT                  text   null,
    CLOSECONTACT             text   null,
    PROBABLE                 text   null,
    suspect_diisolasi        text   null,
    suspect_discarded        text   null,
    closecontact_dikarantina text   null,
    closecontact_discarded   text   null,
    probable_diisolasi       text   null,
    probable_discarded       text   null,
    CONFIRMATION             text   null,
    confirmation_sembuh      text   null,
    confirmation_meninggal   text   null,
    suspect_meninggal        text   null,
    closecontact_meninggal   text   null,
    probable_meninggal       text   null
);

INSERT INTO data_enginner_master.yearly_fact (Tahun, kode_prov, nama_prov, SUSPECT, CLOSECONTACT, PROBABLE, suspect_diisolasi, suspect_discarded, closecontact_dikarantina, closecontact_discarded, probable_diisolasi, probable_discarded, CONFIRMATION, confirmation_sembuh, confirmation_meninggal, suspect_meninggal, closecontact_meninggal, probable_meninggal) VALUES (2020, '32', 'Jawa Barat', '107221', '179071', '219377', '35463', '100562', '55133', '162890', '15039', '97644', '81427', '68645', '1163', '3125', '0', '106694');
INSERT INTO data_enginner_master.yearly_fact (Tahun, kode_prov, nama_prov, SUSPECT, CLOSECONTACT, PROBABLE, suspect_diisolasi, suspect_discarded, closecontact_dikarantina, closecontact_discarded, probable_diisolasi, probable_discarded, CONFIRMATION, confirmation_sembuh, confirmation_meninggal, suspect_meninggal, closecontact_meninggal, probable_meninggal) VALUES (2021, '32', 'Jawa Barat', '109665', '433357', '1847070', '23972', '107756', '72689', '423166', '80132', '1132586', '625250', '622761', '13584', '0', '0', '634299');
INSERT INTO data_enginner_master.yearly_fact (Tahun, kode_prov, nama_prov, SUSPECT, CLOSECONTACT, PROBABLE, suspect_diisolasi, suspect_discarded, closecontact_dikarantina, closecontact_discarded, probable_diisolasi, probable_discarded, CONFIRMATION, confirmation_sembuh, confirmation_meninggal, suspect_meninggal, closecontact_meninggal, probable_meninggal) VALUES (2022, '32', 'Jawa Barat', '0', '0', '2108890', '0', '0', '0', '0', '90804', '1315671', '505190', '488222', '1272', '0', '0', '702415');
