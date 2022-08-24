package sql;

/**
 * Defines the protocols for a field type.
 * <p>
 * Do not modify existing protocols,
 * but you may add new protocols.
 */
public enum FieldType {
	STRING(1),
	INTEGER(2),
	BOOLEAN(3);

	private int type;

	private FieldType(int type) {
        this.type = type;
    }

	public String getName() {
        return name();
    }

	public int getTypeNumber() {
		return type;
	}

    public static FieldType valueOf(int type) {
        for (FieldType it: FieldType.class.getEnumConstants()) {
            if (type == it.type)
                return it;
        }
        throw new IllegalArgumentException("Unknown type <%s>".formatted(type));
    }
}
