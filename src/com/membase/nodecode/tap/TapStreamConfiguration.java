/*
 * Copyright (c) 2010 Membase. All Rights Reserved.
 */

package com.membase.nodecode.tap;

import java.util.Date;

/**
 *
 */
public class TapStreamConfiguration {
	public static final Date NOW = new Date(Long.MAX_VALUE);

	private String tapName;
	private String bucketName;
	private String bucketPassword;
	private TapStreamType streamType;
	private long startTime;

	public TapStreamConfiguration(String tapName) {
		setTapName(tapName);
	}

	public TapStreamConfiguration(String tapName, String bucketName,
			String bucketPassword, TapStreamType streamType) {
		this.bucketPassword = bucketPassword;
		setTapName(tapName);
		setBucketName(bucketName);
		this.streamType = streamType;
	}

	private void setTapName(String tapName) {
		if (tapName == null || tapName.isEmpty())
			throw new IllegalArgumentException("tap name may not be null");

		this.tapName = tapName;
	}

	public String getTapName() {
		return tapName;
	}

	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		if (bucketName == null || bucketName.isEmpty())
			throw new IllegalArgumentException("bucket name may not be null");

		this.bucketName = bucketName;
	}

	public String getBucketPassword() {
		return bucketPassword;
	}

	public TapStreamType getStreamType() {
		return streamType;
	}

	public void setStreamType(TapStreamType streamType) {
		this.streamType = streamType;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(Date startTime) {
		if (NOW.equals(startTime))
			this.startTime = -1;
		else
			this.startTime = startTime.getTime();
	}

	public void setStartDate(long startTime) {
		this.startTime = startTime;
	}
}
