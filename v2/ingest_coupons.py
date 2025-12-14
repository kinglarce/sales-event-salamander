"""
Improved Coupon Ingestion
Refactored with senior software engineering best practices.
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
import csv
import os

from core import (
    get_config,
    DatabaseManager,
    TransactionManager,
    VivenuHTTPClient,
    BatchProcessor,
    ProgressTracker,
    PerformanceLogger,
    APILogger,
    get_logger,
    setup_logging,
    retry_on_failure
)
from models.database import Base, CouponSeries, Coupon, CouponUsageSummary


class CouponIngestionError(Exception):
    """Base coupon ingestion error"""
    pass


class CouponAPIError(CouponIngestionError):
    """Coupon API-related errors"""
    pass


class CouponDataError(CouponIngestionError):
    """Coupon data processing errors"""
    pass


@dataclass
class CouponIngestionResult:
    """Result of coupon ingestion process"""
    success: bool
    processed_coupons: int
    processed_series: int
    duration: float
    error_message: Optional[str] = None


class CouponProcessor:
    """Enhanced coupon processor with better error handling"""
    
    def __init__(self, session, schema: str, tracked_codes: Dict[str, str], logger: logging.Logger):
        self.session = session
        self.schema = schema
        self.tracked_codes = tracked_codes
        self.logger = logger
        self.processed = 0
        self.failed = 0
    
    def process_coupon_series(self, series_data: Dict) -> Optional[CouponSeries]:
        """Process coupon series with validation"""
        try:
            series = CouponSeries(
                id=series_data['_id'],
                region_schema=self.schema,
                name=series_data.get('name'),
                active=series_data.get('active', True)
            )
            
            merged_series = self.session.merge(series)
            self.processed += 1
            return merged_series
            
        except Exception as e:
            self.failed += 1
            self.logger.error(f"Error processing series {series_data.get('_id')}: {e}")
            return None
    
    def process_coupon(self, coupon_data: Dict) -> Optional[Coupon]:
        """Process individual coupon with validation"""
        try:
            coupon_code = coupon_data.get('code', '')
            is_tracked = coupon_code in self.tracked_codes
            category = self.tracked_codes.get(coupon_code, '') if is_tracked else None
            is_used = coupon_data.get('used', 0) > 0
            
            coupon = Coupon(
                id=coupon_data['_id'],
                region_schema=self.schema,
                code=coupon_code,
                name=coupon_data.get('name'),
                active=coupon_data.get('active', True),
                used=coupon_data.get('used', 0),
                is_used=is_used,
                is_tracked=is_tracked,
                category=category,
                coupon_series_id=coupon_data.get('couponSeriesId')
            )
            
            merged_coupon = self.session.merge(coupon)
            self.processed += 1
            return merged_coupon
            
        except Exception as e:
            self.failed += 1
            self.logger.error(f"Error processing coupon {coupon_data.get('_id')}: {e}")
            return None


class CouponDataLoader:
    """Loads and manages distributed coupon codes"""
    
    def __init__(self, schema: str, logger: logging.Logger):
        self.schema = schema
        self.logger = logger
    
    def load_distributed_codes(self) -> Dict[str, str]:
        """Load distributed coupon codes from CSV file"""
        distributed_codes = {}
        csv_path = f"data_static/coupons/{self.schema}-distributed.csv"
        
        if not os.path.exists(csv_path):
            self.logger.warning(f"Tracked codes file not found: {csv_path}")
            return distributed_codes
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    if 'Code' in row and row['Code']:
                        code = row['Code'].strip()
                        category = row.get('Category', '').strip()
                        
                        # Clean up category
                        if category:
                            category = category.replace('/', '-').replace(' ', '-')
                            category = '-'.join(filter(None, category.split('-')))
                        
                        distributed_codes[code] = category
            
            self.logger.info(f"Loaded {len(distributed_codes)} tracked codes from {csv_path}")
            
        except Exception as e:
            self.logger.error(f"Error loading tracked codes from {csv_path}: {e}")
        
        return distributed_codes


class CouponUsageSummaryUpdater:
    """Updates coupon usage summary statistics"""
    
    def __init__(self, session, schema: str, logger: logging.Logger):
        self.session = session
        self.schema = schema
        self.logger = logger
    
    @retry_on_failure(max_retries=3)
    def update_summary(self):
        """Update coupon usage summary with retry logic"""
        try:
            # Get summary data by series
            summary_query = f"""
                SELECT 
                    cs.id as series_id,
                    cs.name as series_name,
                    COUNT(c.id) as total_codes,
                    SUM(CASE WHEN c.is_used THEN 1 ELSE 0 END) as used_codes,
                    SUM(CASE WHEN NOT c.is_used THEN 1 ELSE 0 END) as unused_codes,
                    SUM(CASE WHEN c.is_tracked THEN 1 ELSE 0 END) as tracked_codes,
                    SUM(CASE WHEN c.is_tracked AND c.is_used THEN 1 ELSE 0 END) as tracked_used_codes,
                    SUM(CASE WHEN c.is_tracked AND NOT c.is_used THEN 1 ELSE 0 END) as tracked_unused_codes
                FROM {self.schema}.coupon_series cs
                LEFT JOIN {self.schema}.coupons c ON cs.id = c.coupon_series_id
                GROUP BY cs.id, cs.name
            """
            
            results = self.session.execute(summary_query).fetchall()
            
            # Update or create summary records
            for row in results:
                summary_id = f"{row.series_id}_{self.schema}"
                
                summary = self.session.get(CouponUsageSummary, summary_id)
                if summary:
                    # Update existing summary
                    summary.total_codes = row.total_codes or 0
                    summary.used_codes = row.used_codes or 0
                    summary.unused_codes = row.unused_codes or 0
                    summary.tracked_codes = row.tracked_codes or 0
                    summary.tracked_used_codes = row.tracked_used_codes or 0
                    summary.tracked_unused_codes = row.tracked_unused_codes or 0
                    summary.updated_at = datetime.now()
                else:
                    # Create new summary
                    summary = CouponUsageSummary(
                        id=summary_id,
                        region_schema=self.schema,
                        series_id=row.series_id,
                        series_name=row.series_name,
                        total_codes=row.total_codes or 0,
                        used_codes=row.used_codes or 0,
                        unused_codes=row.unused_codes or 0,
                        tracked_codes=row.tracked_codes or 0,
                        tracked_used_codes=row.tracked_used_codes or 0,
                        tracked_unused_codes=row.tracked_unused_codes or 0
                    )
                    self.session.add(summary)
            
            self.session.commit()
            self.logger.info(f"Updated coupon usage summary for schema: {self.schema}")
            
        except Exception as e:
            self.session.rollback()
            self.logger.error(f"Error updating coupon usage summary: {e}")
            raise


class CouponIngester:
    """Main coupon ingestion class with improved architecture"""
    
    def __init__(self, config):
        self.config = config
        self.logger = get_logger(__name__)
        self.performance_logger = PerformanceLogger(self.logger)
        self.api_logger = APILogger(self.logger)
    
    async def ingest_coupon_data(
        self, 
        token: str, 
        event_id: str, 
        schema: str
    ) -> CouponIngestionResult:
        """Main coupon ingestion method"""
        start_time = datetime.now()
        self.performance_logger.start_timer("coupon_ingestion")
        
        try:
            # Setup database
            db_manager = DatabaseManager(self.config.database)
            db_manager.setup_schema()
            
            # Load tracked codes
            data_loader = CouponDataLoader(schema, self.logger)
            tracked_codes = data_loader.load_distributed_codes()
            
            # Process with API
            async with VivenuHTTPClient(token, self.config.events[0].base_url) as api:
                # Process coupon series
                series_count = await self._process_coupon_series(api, db_manager, schema)
                
                # Process individual coupons
                coupon_count = await self._process_coupons(api, db_manager, event_id, schema, tracked_codes)
                
                # Update usage summary
                await self._update_usage_summary(db_manager, schema)
                
                duration = (datetime.now() - start_time).total_seconds()
                self.performance_logger.end_timer(
                    "coupon_ingestion", 
                    processed_series=series_count,
                    processed_coupons=coupon_count
                )
                
                return CouponIngestionResult(
                    success=True,
                    processed_coupons=coupon_count,
                    processed_series=series_count,
                    duration=duration
                )
                
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            self.logger.error(f"Coupon ingestion failed: {e}")
            
            return CouponIngestionResult(
                success=False,
                processed_coupons=0,
                processed_series=0,
                duration=duration,
                error_message=str(e)
            )
    
    async def _process_coupon_series(
        self, 
        api: VivenuHTTPClient, 
        db_manager: DatabaseManager, 
        schema: str
    ) -> int:
        """Process coupon series"""
        try:
            series_data = await api.get_coupon_series()
            series_list = series_data.get("docs", [])
            
            processed_count = 0
            
            with TransactionManager(db_manager) as session:
                processor = CouponProcessor(session, schema, {}, self.logger)
                
                for series in series_list:
                    if processor.process_coupon_series(series):
                        processed_count += 1
                
                self.logger.info(f"Processed {processed_count} coupon series")
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error processing coupon series: {e}")
            raise CouponAPIError(f"Failed to process coupon series: {e}")
    
    async def _process_coupons(
        self, 
        api: VivenuHTTPClient, 
        db_manager: DatabaseManager, 
        event_id: str, 
        schema: str, 
        tracked_codes: Dict[str, str]
    ) -> int:
        """Process individual coupons with batch processing"""
        try:
            # Get total count
            first_batch = await api.get_coupons(event_id, skip=0, limit=1)
            total_coupons = first_batch.get("total", 0)
            
            if not total_coupons:
                self.logger.warning("No coupons found to process")
                return 0
            
            # Process coupons in batches
            processed_count = 0
            batch_size = self.config.batch.batch_size
            
            for skip in range(0, total_coupons, batch_size):
                try:
                    # Fetch batch
                    batch_data = await api.get_coupons(event_id, skip=skip, limit=batch_size)
                    coupons = batch_data.get("rows", [])
                    
                    if not coupons:
                        break
                    
                    # Process batch
                    with TransactionManager(db_manager) as session:
                        processor = CouponProcessor(session, schema, tracked_codes, self.logger)
                        
                        for coupon in coupons:
                            processor.process_coupon(coupon)
                        
                        processed_count += processor.processed
                        self.logger.info(f"Processed batch: {processor.processed} coupons, {processor.failed} failed")
                
                except Exception as e:
                    self.logger.error(f"Error processing coupon batch at skip {skip}: {e}")
                    continue
            
            return processed_count
            
        except Exception as e:
            self.logger.error(f"Error processing coupons: {e}")
            raise CouponAPIError(f"Failed to process coupons: {e}")
    
    async def _update_usage_summary(self, db_manager: DatabaseManager, schema: str):
        """Update coupon usage summary"""
        with TransactionManager(db_manager) as session:
            updater = CouponUsageSummaryUpdater(session, schema, self.logger)
            updater.update_summary()


async def main():
    """Main entry point"""
    # Load configuration
    config = get_config()
    
    # Setup logging
    setup_logging(config.logging)
    logger = get_logger(__name__)
    
    # Process each event configuration
    ingester = CouponIngester(config)
    
    for event_config in config.events:
        try:
            logger.info(f"Processing coupon data for schema: {event_config.schema}")
            
            result = await ingester.ingest_coupon_data(
                token=event_config.token,
                event_id=event_config.event_id,
                schema=event_config.schema
            )
            
            if result.success:
                logger.info(
                    f"Successfully processed {result.processed_coupons} coupons "
                    f"and {result.processed_series} series for {event_config.schema}"
                )
            else:
                logger.error(f"Failed to process {event_config.schema}: {result.error_message}")
                
        except Exception as e:
            logger.error(f"Failed to process schema {event_config.schema}: {e}")
            continue


if __name__ == "__main__":
    asyncio.run(main())
