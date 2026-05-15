"""
Data models for Collibra Connector response objects.

Provides Pydantic models (or simple dataclass-like objects) for structuring
API responses into well-typed Python objects.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime

try:
    from pydantic import BaseModel, Field
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False
    
    # Fallback implementation without Pydantic
    class BaseModel:
        """Fallback base model when Pydantic is not available."""
        def __init__(self, **data):
            for key, value in data.items():
                setattr(self, key, value)
        
        def __repr__(self):
            attrs = ', '.join(f"{k}={v!r}" for k, v in self.__dict__.items())
            return f"{self.__class__.__name__}({attrs})"
        
        class Config:
            arbitrary_types_allowed = True

    def Field(**kwargs):
        """Fallback Field function."""
        return kwargs.get('default_factory', kwargs.get('default', None))


class AssetModel(BaseModel):
    """Model representing a basic asset."""
    id: str
    name: str
    display_name: Optional[str] = None
    type_name: Optional[str] = None
    type_id: Optional[str] = None
    status_name: Optional[str] = None
    status_id: Optional[str] = None
    domain_name: Optional[str] = None
    domain_id: Optional[str] = None
    created_on: Optional[datetime] = None
    last_modified_on: Optional[datetime] = None
    type: Optional[Dict[str, Any]] = None
    status: Optional[Dict[str, Any]] = None
    domain: Optional[Dict[str, Any]] = None

    if PYDANTIC_AVAILABLE:
        class Config:
            arbitrary_types_allowed = True


class ResponsibilitySummary(BaseModel):
    """Model representing an asset responsibility."""
    role: str
    owner: str
    owner_id: Optional[str] = None


class CommentModel(BaseModel):
    """Model representing a comment on an asset."""
    id: str
    content: str
    created_by: Optional[str] = None
    created_date: Optional[datetime] = None
    resolved: Optional[bool] = False

    if PYDANTIC_AVAILABLE:
        class Config:
            arbitrary_types_allowed = True


class RelationsGrouped(BaseModel):
    """Model representing grouped relations for an asset."""
    outgoing: Dict[str, List[Dict[str, Any]]] = None
    incoming: Dict[str, List[Dict[str, Any]]] = None
    outgoing_count: int = 0
    incoming_count: int = 0
    
    def __init__(self, **data):
        if 'outgoing' not in data:
            data['outgoing'] = {}
        if 'incoming' not in data:
            data['incoming'] = {}
        super().__init__(**data)


class AssetProfileModel(BaseModel):
    """Model representing a complete asset profile."""
    asset: AssetModel
    attributes: Dict[str, Any] = None
    relations: RelationsGrouped = None
    responsibilities: List[ResponsibilitySummary] = None
    comments: List[CommentModel] = None
    activities: List[Dict[str, Any]] = None
    tags: List[str] = None
    attachments: List[Dict[str, Any]] = None
    
    def __init__(self, **data):
        if 'attributes' not in data:
            data['attributes'] = {}
        if 'relations' not in data:
            data['relations'] = RelationsGrouped()
        if 'responsibilities' not in data:
            data['responsibilities'] = []
        if 'comments' not in data:
            data['comments'] = []
        if 'activities' not in data:
            data['activities'] = []
        if 'tags' not in data:
            data['tags'] = []
        if 'attachments' not in data:
            data['attachments'] = []
        super().__init__(**data)

