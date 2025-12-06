"""
Ratings Service for MediaCircle

Handles all rating-related operations including:
- Submitting user ratings
- Aggregating item ratings
- Managing user rating lists
"""

import logging
from datetime import datetime

from firebase_admin import firestore

from utils.mc_types import (
    ItemRating,
    Rating,
    RatingSubmission,
    UserRatingItem,
    get_rating_score,
)

logger = logging.getLogger(__name__)


def get_firestore_client():
    """Get Firestore client instance."""
    return firestore.client()


def generate_rating_id(mc_id: str, user_id: str) -> str:
    """
    Generate a deterministic rating ID from mc_id and user_id.

    Args:
        mc_id: The MediaCircle item ID
        user_id: The user's ID

    Returns:
        str: Deterministic rating ID in format "mc_id_user_id"
    """
    return f"{mc_id}_{user_id}"


class RatingsService:
    """Service for managing user ratings and item rating aggregations."""

    def __init__(self):
        """Initialize the ratings service."""
        self._db = None
        logger.info("RatingsService initialized")

    @property
    def db(self):
        """Lazy-load Firestore client to avoid initialization order issues."""
        if self._db is None:
            self._db = get_firestore_client()
        return self._db

    def submit_rating(self, submission: RatingSubmission) -> Rating:
        """
        Submit a user rating for an item.

        This performs the following operations:
        1. Creates/updates the rating document in ratings/{rating_id}
        2. Updates the aggregated rating in item_ratings/{mc_id}
        3. Updates the user's ratings array in users/{uid}/private/settings

        Args:
            submission: RatingSubmission dataclass with all required data

        Returns:
            Rating: The created/updated rating

        Raises:
            ValueError: If rating_unicode is invalid
        """
        try:
            # Validate rating
            submission.validate()
            rating_score = get_rating_score(submission.rating_unicode)

            rating_id = generate_rating_id(submission.mc_id, submission.user_id)
            timestamp = datetime.utcnow().isoformat() + "Z"

            # Create Rating object
            rating = Rating(
                rating_id=rating_id,
                user_id=submission.user_id,
                mc_id=submission.mc_id,
                rating=rating_score,
                rating_unicode=submission.rating_unicode,
                timestamp=timestamp,
            )

            # 1. Create/update the rating document
            self.db.collection("ratings").document(rating_id).set(
                rating.to_dict(), merge=True
            )

            logger.info(f"Rating saved: {rating_id} - Score: {rating_score}")

            # 2. Update aggregated rating
            self._update_item_rating_aggregation(
                submission.mc_id,
                submission.mc_type,
                submission.mc_title,
                submission.mc_image,
                rating_score,
                timestamp,
            )

            # 3. Update user's ratings list
            self._update_user_ratings_list(submission.user_id, rating_id)

            return rating

        except Exception as e:
            logger.error(f"Error submitting rating: {str(e)}")
            raise

    def _update_item_rating_aggregation(
        self,
        mc_id: str,
        mc_type: str,
        mc_title: str,
        mc_image: str,
        new_rating: int,
        timestamp: str,
    ) -> None:
        """
        Update the aggregated rating for an item.

        This recalculates the average rating and count for the item
        by querying all ratings for that mc_id.

        Args:
            mc_id: The MediaCircle item ID
            mc_type: The item type
            mc_title: The item title
            mc_image: The item image URL
            new_rating: The new rating score being added
            timestamp: ISO timestamp of the rating
        """
        try:
            # Query all ratings for this item
            ratings_query = self.db.collection("ratings").where(
                filter=firestore.FieldFilter("mc_id", "==", mc_id)
            )
            ratings_docs = ratings_query.stream()

            # Calculate aggregates
            total_score = 0
            count = 0

            for doc in ratings_docs:
                data = doc.to_dict()
                total_score += data.get("rating", 0)
                count += 1

            average = round(total_score / count, 2) if count > 0 else 0.0

            # Create ItemRating object
            item_rating = ItemRating(
                mc_id=mc_id,
                mc_type=mc_type,
                mc_title=mc_title,
                mc_image=mc_image,
                count=count,
                average=average,
                last_rated=timestamp,
            )

            # Update item_ratings document
            self.db.collection("item_ratings").document(mc_id).set(
                item_rating.to_dict(), merge=True
            )

            logger.info(
                f"Item rating updated: {mc_id} - Count: {count}, Avg: {average}"
            )

        except Exception as e:
            logger.error(f"Error updating item rating aggregation: {str(e)}")
            raise

    def _update_user_ratings_list(self, user_id: str, rating_id: str) -> None:
        """
        Update the user's ratings list in their private settings.

        Args:
            user_id: The user's UID
            rating_id: The rating ID to add to the list
        """
        try:
            settings_ref = self.db.collection("users").document(user_id).collection(
                "private"
            ).document("settings")

            # Use arrayUnion to add rating_id if it doesn't exist
            settings_ref.set(
                {"ratings": firestore.ArrayUnion([rating_id])}, merge=True
            )

            logger.info(f"User ratings list updated: {user_id} - Added: {rating_id}")

        except Exception as e:
            logger.error(f"Error updating user ratings list: {str(e)}")
            raise

    def get_user_ratings(self, user_id: str) -> list[UserRatingItem]:
        """
        Get all ratings for a specific user.

        Returns minimal item data including mc_id, mc_type, image, and title.

        Args:
            user_id: The user's UID

        Returns:
            list: List of UserRatingItem objects
        """
        try:
            # Query all ratings for this user
            ratings_query = self.db.collection("ratings").where(
                filter=firestore.FieldFilter("userId", "==", user_id)
            ).order_by("timestamp", direction=firestore.Query.DESCENDING)

            ratings_docs = ratings_query.stream()

            results: list[UserRatingItem] = []
            for doc in ratings_docs:
                rating_data = doc.to_dict()
                if not rating_data:
                    continue

                rating_data["rating_id"] = doc.id
                rating = Rating.from_dict(rating_data)

                # Get item metadata from item_ratings collection
                item_doc = self.db.collection("item_ratings").document(rating.mc_id).get()
                item_data = item_doc.to_dict() if item_doc.exists else {}

                # Create UserRatingItem
                user_rating_item = UserRatingItem.from_rating_and_item(rating, item_data)
                results.append(user_rating_item)

            logger.info(f"Retrieved {len(results)} ratings for user {user_id}")
            return results

        except Exception as e:
            logger.error(f"Error getting user ratings: {str(e)}")
            raise

    def get_item_ratings(self, mc_id: str) -> ItemRating:
        """
        Get aggregated rating data for a specific item.

        Args:
            mc_id: The MediaCircle item ID

        Returns:
            ItemRating: Aggregated rating data
        """
        try:
            item_doc = self.db.collection("item_ratings").document(mc_id).get()

            if not item_doc.exists:
                # Return empty ItemRating
                return ItemRating(
                    mc_id=mc_id,
                    mc_type="",
                    mc_title="",
                    mc_image="",
                    count=0,
                    average=0.0,
                    last_rated=None,
                )

            data = item_doc.to_dict()
            if not data:
                return ItemRating(
                    mc_id=mc_id,
                    mc_type="",
                    mc_title="",
                    mc_image="",
                    count=0,
                    average=0.0,
                    last_rated=None,
                )

            data["mc_id"] = mc_id
            item_rating = ItemRating.from_dict(data)

            logger.info(f"Retrieved rating data for item {mc_id}")
            return item_rating

        except Exception as e:
            logger.error(f"Error getting item ratings: {str(e)}")
            raise

    def get_user_rating_for_item(
        self, user_id: str, mc_id: str
    ) -> Rating | None:
        """
        Get a specific user's rating for a specific item.

        Args:
            user_id: The user's UID
            mc_id: The MediaCircle item ID

        Returns:
            Rating or None: The rating if it exists, None otherwise
        """
        try:
            rating_id = generate_rating_id(mc_id, user_id)
            rating_doc = self.db.collection("ratings").document(rating_id).get()

            if not rating_doc.exists:
                return None

            data = rating_doc.to_dict()
            if not data:
                return None

            data["rating_id"] = rating_id
            rating = Rating.from_dict(data)

            logger.info(f"Retrieved rating for user {user_id}, item {mc_id}")
            return rating

        except Exception as e:
            logger.error(f"Error getting user rating for item: {str(e)}")
            raise

    def delete_rating(self, user_id: str, mc_id: str) -> str:
        """
        Delete a user's rating for an item.

        This performs the following operations:
        1. Deletes the rating document
        2. Updates the aggregated rating
        3. Removes from user's ratings list

        Args:
            user_id: The user's UID
            mc_id: The MediaCircle item ID

        Returns:
            str: The deleted rating_id

        Raises:
            ValueError: If rating not found
        """
        try:
            rating_id = generate_rating_id(mc_id, user_id)

            # Get the rating before deleting to get item metadata
            rating_doc = self.db.collection("ratings").document(rating_id).get()
            if not rating_doc.exists:
                raise ValueError("Rating not found")

            # 1. Delete the rating document
            self.db.collection("ratings").document(rating_id).delete()

            logger.info(f"Rating deleted: {rating_id}")

            # 2. Recalculate item rating aggregation
            # Get item data from item_ratings to maintain metadata
            item_doc = self.db.collection("item_ratings").document(mc_id).get()
            if item_doc.exists:
                item_data = item_doc.to_dict()
                if item_data:
                    self._recalculate_item_rating(
                        mc_id,
                        item_data.get("mc_type", ""),
                        item_data.get("mc_title", ""),
                        item_data.get("mc_image", ""),
                    )

            # 3. Remove from user's ratings list
            settings_ref = self.db.collection("users").document(user_id).collection(
                "private"
            ).document("settings")

            # Use set with merge=True to handle case where settings doesn't exist
            settings_ref.set(
                {"ratings": firestore.ArrayRemove([rating_id])}, merge=True
            )

            return rating_id

        except Exception as e:
            logger.error(f"Error deleting rating: {str(e)}")
            raise

    def _recalculate_item_rating(
        self, mc_id: str, mc_type: str, mc_title: str, mc_image: str
    ) -> None:
        """
        Recalculate the aggregated rating for an item after a deletion.

        Args:
            mc_id: The MediaCircle item ID
            mc_type: The item type
            mc_title: The item title
            mc_image: The item image URL
        """
        try:
            # Query all ratings for this item
            ratings_query = self.db.collection("ratings").where(
                filter=firestore.FieldFilter("mc_id", "==", mc_id)
            )
            ratings_docs = ratings_query.stream()

            # Calculate aggregates
            total_score = 0
            count = 0
            last_timestamp = None

            for doc in ratings_docs:
                data = doc.to_dict()
                if not data:
                    continue
                total_score += data.get("rating", 0)
                count += 1
                timestamp = data.get("timestamp")
                if not last_timestamp or (timestamp and timestamp > last_timestamp):
                    last_timestamp = timestamp

            if count > 0:
                average = round(total_score / count, 2)
                # Create ItemRating and update
                item_rating = ItemRating(
                    mc_id=mc_id,
                    mc_type=mc_type,
                    mc_title=mc_title,
                    mc_image=mc_image,
                    count=count,
                    average=average,
                    last_rated=last_timestamp,
                )
                self.db.collection("item_ratings").document(mc_id).set(
                    item_rating.to_dict(),
                    merge=True,
                )
            else:
                # No ratings left, delete the item_ratings document
                self.db.collection("item_ratings").document(mc_id).delete()

            logger.info(
                f"Item rating recalculated: {mc_id} - Count: {count}, Avg: {average if count > 0 else 0}"
            )

        except Exception as e:
            logger.error(f"Error recalculating item rating: {str(e)}")
            raise
