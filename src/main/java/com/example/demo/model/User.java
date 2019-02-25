package com.example.demo.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import twitter4j.Twitter;

@JsonIgnoreProperties(ignoreUnknown=true)

public class User {
	
	private String  description;
	private long    followersCount;
	private boolean isProtected;
	private String  location;
	private String  name;
	private static String possible_root_names;
	private String  profileImageUrl;
	private String  screenName;
	private static long serialVersionUID;
	private String  statusCreatedAt;
	private boolean statusFavorited;
	private long    statusId;
	private String  statusInReplyToScreenName;
	private long    statusInReplyToStatusId;
	private long    statusInReplyToUserId;
	private String  statusSource;
	private String  statusText;
	private boolean statusTruncated;
	private Twitter twitter;
	private String  url;
	
	
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	public long getFollowersCount() {
		return followersCount;
	}
	public void setFollowersCount(long followersCount) {
		this.followersCount = followersCount;
	}
	public boolean isProtected() {
		return isProtected;
	}
	public void setProtected(boolean isProtected) {
		this.isProtected = isProtected;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public static String getPossible_root_names() {
		return possible_root_names;
	}
	public static void setPossible_root_names(String possible_root_names) {
		User.possible_root_names = possible_root_names;
	}
	public String getProfileImageUrl() {
		return profileImageUrl;
	}
	public void setProfileImageUrl(String profileImageUrl) {
		this.profileImageUrl = profileImageUrl;
	}
	public String getScreenName() {
		return screenName;
	}
	public void setScreenName(String screenName) {
		this.screenName = screenName;
	}
	public static long getSerialVersionUID() {
		return serialVersionUID;
	}
	public static void setSerialVersionUID(long serialVersionUID) {
		User.serialVersionUID = serialVersionUID;
	}
	public String getStatusCreatedAt() {
		return statusCreatedAt;
	}
	public void setStatusCreatedAt(String statusCreatedAt) {
		this.statusCreatedAt = statusCreatedAt;
	}
	public boolean isStatusFavorited() {
		return statusFavorited;
	}
	public void setStatusFavorited(boolean statusFavorited) {
		this.statusFavorited = statusFavorited;
	}
	public long getStatusId() {
		return statusId;
	}
	public void setStatusId(long statusId) {
		this.statusId = statusId;
	}
	public String getStatusInReplyToScreenName() {
		return statusInReplyToScreenName;
	}
	public void setStatusInReplyToScreenName(String statusInReplyToScreenName) {
		this.statusInReplyToScreenName = statusInReplyToScreenName;
	}
	public long getStatusInReplyToStatusId() {
		return statusInReplyToStatusId;
	}
	public void setStatusInReplyToStatusId(long statusInReplyToStatusId) {
		this.statusInReplyToStatusId = statusInReplyToStatusId;
	}
	public long getStatusInReplyToUserId() {
		return statusInReplyToUserId;
	}
	public void setStatusInReplyToUserId(long statusInReplyToUserId) {
		this.statusInReplyToUserId = statusInReplyToUserId;
	}
	public String getStatusSource() {
		return statusSource;
	}
	public void setStatusSource(String statusSource) {
		this.statusSource = statusSource;
	}
	public String getStatusText() {
		return statusText;
	}
	public void setStatusText(String statusText) {
		this.statusText = statusText;
	}
	public boolean isStatusTruncated() {
		return statusTruncated;
	}
	public void setStatusTruncated(boolean statusTruncated) {
		this.statusTruncated = statusTruncated;
	}
	public Twitter getTwitter() {
		return twitter;
	}
	public void setTwitter(Twitter twitter) {
		this.twitter = twitter;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}

}
