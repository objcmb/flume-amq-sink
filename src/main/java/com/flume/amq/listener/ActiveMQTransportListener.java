/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */
package com.flume.amq.listener;

import java.io.IOException;

import org.apache.activemq.transport.TransportListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @version:1.0
 * 
 */
public class ActiveMQTransportListener implements TransportListener{
	private Logger logger = LoggerFactory.getLogger(ActiveMQTransportListener.class);
		/**     
		 * @param command     
		 */     
		@Override     
		public void onCommand(Object o) {
			
		}     
		 
		/**     
		 * @param error     
		 */     
		@Override     
		public void onException(IOException e) {
//			LogUtil.error(logger, ExceptionConst.AMQ_MOUDLE, ExceptionConst.AMQ_TRANSPORT_EXCEPTION,"AMQ,队列出现异常,",e);
		}     
		 
		/**     
		 */     
		@Override     
		public void transportInterupted() {     
//			LogUtil.error(logger, ExceptionConst.AMQ_MOUDLE, ExceptionConst.AMQ_TRANSPORT_INTERUPTED,"AMQ,transportInterupted连接发生中断");
		}     
		 
		/**     
		 */     
		@Override     
		public void transportResumed() {     
//			LogUtil.error(logger, ExceptionConst.AMQ_MOUDLE, ExceptionConst.AMQ_TRANSPORT_RESUMED,"AMQ,transportResumed连接已恢复");

		}
}
