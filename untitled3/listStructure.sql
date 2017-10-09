companycode  CREATE TABLE `companycode` (
               `Id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
               `uncode` varchar(10) DEFAULT NULL COMMENT '期货统一编码',
               `fucompany` varchar(20) DEFAULT NULL COMMENT '期货公司名称',
               PRIMARY KEY (`Id`)
             ) ENGINE=InnoDB DEFAULT CHARSET=utf8

tradinginfor  CREATE TABLE `tradinginfor` (
                `Id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
                `date` varchar(20) DEFAULT NULL COMMENT '交易时间',
                `buycompany` varchar(20) DEFAULT NULL COMMENT '持买公司',
                `buy` varchar(20) DEFAULT NULL COMMENT '持买量',
                `sellcompany` varchar(20) DEFAULT NULL COMMENT '持卖公司',
                `sell` varchar(20) DEFAULT NULL COMMENT '持卖量',
                PRIMARY KEY (`Id`)
              ) ENGINE=InnoDB AUTO_INCREMENT=7382 DEFAULT CHARSET=utf8

volume  CREATE TABLE `volume` (
          `Id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键Id',
          `date` varchar(20) DEFAULT NULL COMMENT '交易时间',
          `volumecompany` varchar(20) DEFAULT NULL COMMENT '成交量公司',
          `volume` varchar(20) DEFAULT NULL COMMENT '成立量',
          PRIMARY KEY (`Id`)
        ) ENGINE=InnoDB AUTO_INCREMENT=7383 DEFAULT CHARSET=utf8

