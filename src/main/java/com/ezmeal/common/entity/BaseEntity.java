package com.ezmeal.common.entity;

import jakarta.persistence.Access;
import jakarta.persistence.AccessType;
import jakarta.persistence.Column;
import jakarta.persistence.EntityListeners;
import jakarta.persistence.MappedSuperclass;
import java.time.LocalDateTime;
import lombok.Getter;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedBy;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.jpa.domain.support.AuditingEntityListener;

@Getter
@MappedSuperclass
@Access(AccessType.FIELD)
@EntityListeners(AuditingEntityListener.class)
public abstract class BaseEntity {

    @CreatedDate
    @Column(updatable = false)
    protected LocalDateTime createdAt;

    @CreatedBy
    @Column(length = 45, updatable = false)
    protected String createdBy;

    @LastModifiedDate
    @Column
    protected LocalDateTime modifiedAt;

    @LastModifiedBy
    @Column(length = 45)
    protected String modifiedBy;

    @Column(insertable = false)
    protected LocalDateTime deletedAt;

    @Column(length = 45, insertable = false)
    protected String deletedBy;

    public void delete(String deletedBy) {
        if (this.deletedAt != null) {
            return;
        }
        this.deletedAt = LocalDateTime.now();
        this.deletedBy = deletedBy;
    }

    // 요청자가 시스템일 때 사용하는 메서드
    public void deleteBySystem() {
        this.delete("SYSTEM");
    }

    // 요청자가 시스템일 때 사용하는 메서드
    public void setSystemCreated() {
        this.createdBy = "SYSTEM";
        this.modifiedBy = "SYSTEM";
    }

    // 요청자가 시스템일 때 사용하는 메서드
    public void setSystemModified() {
        this.modifiedBy = "SYSTEM";
    }
}
